import logging
import uuid
import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from _constants import SYSTEM_ENV, RequestPath, TOKEN
import request_vo as req_vo
import request_util


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
        self._logger = logging.getLogger("root")
        self._bg_scheduler = BackgroundScheduler()
        self._polling_time = 10
        self._id = str(uuid.uuid4())
        self._bg_scheduler.start()
        self.register_service()

    def is_connected(self) -> bool:
        return self._is_connected

    def register_service(self):
        self._bg_scheduler.add_job(register_service, 'interval', seconds=self._polling_time,
                                   id='register_service', next_run_time=datetime.datetime.now())

    def connection_check(self):
        self._bg_scheduler.add_job(connection_check, 'interval', seconds=self._polling_time,
                                   id="connection_check", next_run_time=datetime.datetime.now())

    def connection_failed(self):
        self._is_connected = False
        self._bg_scheduler.remove_job("connection_check")
        self.register_service()

    def connection_success(self):
        self._is_connected = True
        self.connection_check()
        self._bg_scheduler.remove_job("register_service")

    def log_connection_failed(self):
        self._logger.error(f"can't make connection with API server: {SYSTEM_ENV.API_SERVER} "
                           f"retry after {self._polling_time} seconds..")

    def get_id(self) -> str:
        return self._id


def register_service():
    service_state = ServiceState()
    url = SYSTEM_ENV.API_SERVER + RequestPath.REGISTER_SERVICE
    req_body = req_vo.RegisterService(URL=SYSTEM_ENV.DISCOVER_URL, LABEL=SYSTEM_ENV.NAME, TAG=SYSTEM_ENV.DISCOVER_TAG,
                                      REGION=SYSTEM_ENV.DISCOVER_REGION, ID=service_state.get_id(), TOKEN=TOKEN)
    code, msg = request_util.post_to_system(url=url, data=req_body.dict())
    if code == 0:
        service_state.connection_success()
    else:
        service_state.log_connection_failed()


def connection_check():
    service_state = ServiceState()
    url = SYSTEM_ENV.API_SERVER + RequestPath.CHECK_SERVICE_CONNECTION + f"?sid={service_state.get_id()}"
    code, msg = request_util.get_from_system(url=url)
    if code != 0:
        service_state.connection_failed()
