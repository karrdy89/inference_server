import logging
import datetime
import concurrent.futures

from apscheduler.schedulers.background import BackgroundScheduler
import docker

from _constants import SYSTEM_ENV, RequestPath, TOKEN
import request_vo as req_vo
import request_util
from _types import BaseContainerStats, BaseServerStats
from utils import calculate_network_bytes, calculate_block_bytes, calculate_cpu_usage

logging.getLogger('apscheduler').setLevel(logging.WARN)


def singleton(class_):
    instances = {}

    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return get_instance


@singleton
class ServiceState:
    def __init__(self):
        self._is_connected: bool = False
        self._docker_client = docker.from_env()
        self._logger = logging.getLogger("root")
        self._bg_scheduler = BackgroundScheduler()
        self._id = None
        self._bg_scheduler.start()
        self.register_service()

    def is_connected(self) -> bool:
        return self._is_connected

    def register_service(self):
        self._bg_scheduler.add_job(register_service, 'interval', seconds=SYSTEM_ENV.HC_INTERVAL,
                                   id='register_service', next_run_time=datetime.datetime.now())

    def connection_check(self):
        self._bg_scheduler.add_job(exchange_stats, 'interval', seconds=SYSTEM_ENV.HC_INTERVAL,
                                   id="connection_check", next_run_time=datetime.datetime.now())

    def connection_failed(self):
        self._is_connected = False
        self._bg_scheduler.remove_job("connection_check")
        self.register_service()

    def connection_success(self, sid: str):
        self._id = sid
        self._is_connected = True
        self._bg_scheduler.remove_job("register_service")
        self.connection_check()

    def log_connection_failed(self):
        self._logger.error(f"can't make connection with Target server: {SYSTEM_ENV.API_SERVER} "
                           f"retry after {SYSTEM_ENV.HC_INTERVAL} seconds..")

    def get_id(self) -> str:
        return self._id

    def get_server_stats(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(self.get_stats), executor.submit(self.get_triton_stats)]
            results = [future.result() for future in futures]
        stats = results.pop(0)
        for _ in results:
            stats.STATS.SERVERS.append(_)
        return stats

    def get_stats(self):
        stats = self.get_container_stats(SYSTEM_ENV.CONTAINER_NAME)
        server_stats = BaseServerStats()
        if stats is None:
            return req_vo.ServerStat(NAME=SYSTEM_ENV.NAME, STATS=server_stats)
        else:
            server_stats.HEALTHY = True
            server_stats.DETAILS = stats
            return req_vo.ServerStat(NAME=SYSTEM_ENV.NAME, STATS=server_stats)

    def get_triton_stats(self):
        stats = self.get_container_stats(SYSTEM_ENV.TRITON_CONTAINER_NAME)
        server_stats = BaseServerStats()
        if stats is None:
            return req_vo.ServerStat(NAME=SYSTEM_ENV.TRITON_SERVER_NAME, STATS=server_stats)
        triton_hc_url = SYSTEM_ENV.TRITON_SERVER_URL + RequestPath.TRITON_HEALTH_CHECK_API
        code, msg = request_util.get(url=triton_hc_url)
        if code != 0:
            return req_vo.ServerStat(NAME=SYSTEM_ENV.TRITON_SERVER_NAME, STATS=server_stats)
        server_stats.DETAILS = stats
        server_stats.HEALTHY = True
        return req_vo.ServerStat(NAME=SYSTEM_ENV.TRITON_SERVER_NAME, STATS=server_stats)

    def get_container_stats(self, container_name: str) -> dict | None:
        try:
            inspect = self._docker_client.api.inspect_container(container_name)
        except docker.errors.NotFound:
            return None
        else:
            if not inspect["State"]["Running"]:
                return None
        stats = self._docker_client.api.stats(container=container_name, stream=False, one_shot=False)
        cpu_usage = calculate_cpu_usage(stats=stats)
        mem_usage = stats["memory_stats"]["usage"]
        mem_limit = stats['memory_stats']['limit']
        net_in, net_out = calculate_network_bytes(stats)
        disk_in, disk_out = calculate_block_bytes(stats)
        return BaseContainerStats(CPU_USAGE=cpu_usage, MEM_USAGE=mem_usage, MEM_LIMIT=mem_limit, NET_IN=net_in,
                                  NET_OUT=net_out, DISK_IN=disk_in, DISK_OUT=disk_out).dict()


def register_service():
    service_state = ServiceState()
    url = SYSTEM_ENV.API_SERVER + RequestPath.REGISTER_SERVICE
    if SYSTEM_ENV.DISCOVER_REGION:
        region = SYSTEM_ENV.DISCOVER_REGION
    else:
        region = None
    req_body = req_vo.RegisterService(URL=SYSTEM_ENV.DISCOVER_URL, LABEL=SYSTEM_ENV.NAME, TAG=SYSTEM_ENV.DISCOVER_TAG,
                                      REGION=region, TOKEN=TOKEN)
    code, msg = request_util.post_to_system(url=url, data=req_body.dict())
    if code == 0:
        service_state.connection_success(msg["SID"])
    else:
        service_state.log_connection_failed()


def exchange_stats():
    service_state = ServiceState()
    if SYSTEM_ENV.DISCOVER_REGION:
        region = SYSTEM_ENV.DISCOVER_REGION
    else:
        region = None
    url = SYSTEM_ENV.API_SERVER + RequestPath.CHECK_SERVICE_CONNECTION + f"?sid={service_state.get_id()}"
    server_stats = service_state.get_server_stats()
    req_body = req_vo.ServerStats(SERVER_STATS=server_stats, INTERVAL=SYSTEM_ENV.HC_INTERVAL,
                                  URL=SYSTEM_ENV.DISCOVER_URL, REGION=region)
    code, msg = request_util.post_to_system(url=url, data=req_body.dict())
    if code != 0:
        service_state.connection_failed()
