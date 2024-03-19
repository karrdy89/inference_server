from multiprocessing import Lock
import time
import json
import traceback
from contextlib import contextmanager
from dataclasses import asdict
import copy
import concurrent.futures

from fastapi import Request, BackgroundTasks
from fastapi.responses import JSONResponse

import request_vo as req_vo
import response_vo as res_vo
from utils import singleton, RollbackContext, get_s3_path, create_version_policy, model_config_to_dict, \
    dict_to_model_config, create_service_path, create_kserve_inference_path, make_inference_input, \
    create_input_description, create_output_description, create_service_description, dissemble_service_path, \
    dissemble_kserve_inference_path, create_desc_path
import request_util
from _types import (RequestResult, ModelVersionState, LoadModelState, InferenceServiceDef, ServiceDescription,
                    DATATYPE_MAP, InferenceIO, Agreement, InferenceServiceState, ServiceEndpointInfo,
                    InferenceServiceStateInfo, ClusterInfo, TaskResult, TaskResultCode, LoadEnsembleState,
                    DeleteRoutingState, CreateRoutingState, UpdateRoutingState)
from _constants import (SYSTEM_ENV, RequestPath, MAX_RETRY, RETRY_DELAY, MODEL_CONFIG_FILENAME, ModelStore,
                        PropPath, ROOT_LOGGER)
from rw_util import RWUtil
from service_state import ServiceState


@singleton
class StateManager:
    def __init__(self, app):
        self._logger = ROOT_LOGGER
        self._app = app
        self._rw_util = RWUtil()
        self._cluster: list[str] | None = None
        self._service_state: ServiceState = ServiceState()

        self._lock_cluster_context: Lock = Lock()
        self._inference_service_state: InferenceServiceStateInfo | None = None
        self._modify_count: int = 0

        self._lock_execute_handle: Lock = Lock()
        self._lock_execute_session: Lock = Lock()
        self._cluster_retry_interval: float = 1
        self._execute_accepted: bool = False
        self._execute_taken: set[str] = set()

        self._lock_loaded_models: Lock = Lock()
        self._loaded_models: dict[str, ModelVersionState] = {}

        self._lock_loaded_ensembles: Lock = Lock()
        self._loaded_ensembles: list[str] = []
        self._update_ensemble_state: dict[str, LoadEnsembleState] = {}

        self._lock_update_config: Lock = Lock()
        self._update_config_state: dict[str, LoadModelState] = {}

        self._create_routing_state: dict[str, CreateRoutingState] = {}
        self._delete_routing_state: dict[str, DeleteRoutingState] = {}
        self._update_routing_state: dict[str, UpdateRoutingState] = {}
        self._lock_routing_table_infer: Lock = Lock()
        self._routing_table_infer: dict[str, InferenceServiceDef] = {}

        self._lock_routing_table_desc: Lock = Lock()
        self._routing_table_desc: dict[str, ServiceDescription] = {}

        self._wait_cluster_set()
        self._join_cluster()
        self._init_deploy()

    @contextmanager
    def _consensus_task(self):
        self._lock_execute_session.acquire()
        try:
            with self._consensus_session():
                pass
            yield None
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}")
            self._modify_count -= 1
        finally:
            self._execute_accepted = False
            self._lock_execute_session.release()

    @contextmanager
    def _consensus_session(self):
        self._lock_execute_handle.acquire()
        try:
            self._set_consensus_result()
            self._execute_accepted = True
            self._modify_count += 1
            yield None
        finally:
            self._lock_execute_handle.release()

    def acquire_handle(self, url) -> int:
        with self._get_lock_execute_handle() as locked:
            if not locked:
                if url in self._execute_taken:
                    return Agreement.AGREE
                else:
                    return Agreement.DISAGREE
            else:
                if self._execute_accepted:
                    return Agreement.REJECT
                else:
                    self._execute_taken.add(url)
                    return Agreement.AGREE

    def _set_consensus_result(self):
        if len(self._cluster) <= 1:
            return
        max_retry = int(SYSTEM_ENV.CLUSTER_REQUEST_TIMEOUT / self._cluster_retry_interval)
        retry_count = 0
        cluster_order = sorted(self._cluster)
        cluster = self._cluster.copy()
        cluster.remove(SYSTEM_ENV.DISCOVER_URL)
        while True:
            if retry_count >= max_retry:
                raise TimeoutError(f"failed to get task handle. time out on get_consensus_result")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                result = list(executor.map(get_agreement, cluster))
            for agreement, url in result:
                if agreement == Agreement.REJECT:
                    time.sleep(self._cluster_retry_interval)
                    retry_count += 1
                    break
                elif agreement == Agreement.DISAGREE:
                    if url in self._execute_taken:
                        time.sleep(self._cluster_retry_interval)
                        retry_count += 1
                        break
                    else:
                        if cluster_order.index(url) < cluster_order.index(SYSTEM_ENV.DISCOVER_URL):
                            time.sleep(self._cluster_retry_interval)
                            retry_count += 1
                            break
                        else:
                            pass
                else:
                    if url in self._execute_taken:
                        self._execute_taken.remove(url)
            else:
                return

    @contextmanager
    def _get_lock_execute_handle(self):
        lock = self._lock_execute_handle.acquire(block=False)
        try:
            yield lock
        finally:
            if lock:
                self._lock_execute_handle.release()

    def _wait_cluster_set(self):
        if SYSTEM_ENV.STAND_ALONE:
            self._cluster = []
            return
        retry_count = 0
        while True:
            if retry_count > MAX_RETRY:
                raise RuntimeError("failed to get cluster information from api server. max retry exceeded.")
            cluster = self._service_state.get_cluster()
            if cluster is None:
                time.sleep(RETRY_DELAY)
                retry_count += 1
            else:
                self._cluster = cluster
                break

    def _join_cluster(self):
        path = PropPath.JOIN_CLUSTER + f"?url={SYSTEM_ENV.DISCOVER_URL}"
        for url in self._cluster:
            if url == SYSTEM_ENV.DISCOVER_URL:
                continue
            url = url + path
            code, msg = request_util.get_from_system(url)
            if code == 0:
                states: InferenceServiceState = InferenceServiceState(**msg["SERVICE_STATE"])
                if states.MODIFY_COUNT > self._modify_count:
                    self._modify_count = states.MODIFY_COUNT
                    self._inference_service_state = states.STATE
            else:
                raise RuntimeError(f"failed to join cluster on '{url}'. '{msg}'")

    def _init_deploy(self):
        # return # dev
        init_by_cluster = False
        is_deploy_info_exist = False
        if self._inference_service_state is not None:
            models = self._inference_service_state.MODELS
            pipelines = self._inference_service_state.PIPELINES
            services = self._inference_service_state.SERVICES
            is_deploy_info_exist = True
            init_by_cluster = True
        if SYSTEM_ENV.API_SERVER is not None:
            if not init_by_cluster:
                url_init_loaded = SYSTEM_ENV.API_SERVER + RequestPath.INIT_SERVICES + f"?region={SYSTEM_ENV.DISCOVER_REGION}"
                code, msg = request_util.get_from_system(url=url_init_loaded)
                if code != 0:
                    raise RuntimeError(f"failed to initiate deploy state")
                models = msg["MODELS"]
                pipelines = msg["PIPELINES"]
                services = msg["SERVICES"]
                is_deploy_info_exist = True

        if not is_deploy_info_exist:
            raise RuntimeError("failed to get deploy information from server.")

        for model_key, model_info in models.items():
            version_state = model_info["version_state"]
            version_latest = model_info["latest"]

            url_load_model = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/load"
            code, msg = request_util.post(url=url_load_model)
            if code != 0:
                raise RuntimeError(f"failed to initiate deploy state. failed to load model {msg}")
            self._loaded_models[model_key] = ModelVersionState(states=version_state, latest=version_latest)

        for pipeline_key in pipelines:
            url_load_model = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{pipeline_key}/load"
            code, msg = request_util.post(url=url_load_model)
            if code != 0:
                raise RuntimeError(f"failed to initiate deploy state. failed to load model {msg}")
            self._loaded_ensembles.append(pipeline_key)

        for service in services:
            service_name = service["SVC_NM"]
            project_id = service["PRJ_ID"]
            model_key = service["MDL_KEY"]
            version = service["VERSION"]
            use_sequence = service["USE_SEQUENCE"]
            input_schema = service["INPUT_SCHEMA"]
            output_schema = service["OUTPUT_SCHEMA"]

            path_infer, path_desc = create_service_path(project=project_id, name=service_name)
            path = create_kserve_inference_path(model_name=model_key, version=version)
            infer_schema = []
            for inference_io in input_schema:
                infer_schema.append((inference_io["name"], DATATYPE_MAP[inference_io["datatype"]]))
            if use_sequence:
                url = get_suitable_server() + path
                inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema, url=url)
                self._app.add_api_route(path_infer, self._make_inference_sequence, methods=["POST"])
            else:
                inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema)
                self._app.add_api_route(path_infer, self._make_inference, methods=["POST"])
            self._routing_table_infer[path_infer] = inference_service_definition

            if init_by_cluster:
                input_desc = input_schema
            else:
                desc_input_schema = []
                for io_dict in input_schema:
                    desc_input_schema.append(InferenceIO(**io_dict))
                input_desc = create_input_description(input_schema=desc_input_schema)

            desc_output_schema = []
            for io_dict in output_schema:
                desc_output_schema.append(InferenceIO(**io_dict))
            output_desc = create_output_description(output_schema=desc_output_schema)
            inference_service_description = create_service_description(url=SYSTEM_ENV.DISCOVER_URL + path_infer,
                                                                       input_desc=input_desc,
                                                                       output_desc=output_desc)
            self._app.add_api_route(path_desc, self._get_service_description, methods=["GET"],
                                    response_model=res_vo.ServiceDescribe)
            self._routing_table_desc[path_desc] = inference_service_description
        self._logger.info(f"initiate service state success.\n"
                          f" --loaded models: {self._loaded_models}\n"
                          f" --routing table(infer): {self._routing_table_infer}\n"
                          f" --routing table(desc): {self._routing_table_desc}")
        self._is_ready = True

    def append_cluster(self, url) -> TaskResult:
        task_result = TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        with self._lock_execute_handle:
            get_cluster_url = SYSTEM_ENV.API_SERVER + RequestPath.GET_CLUSTERS
            data = ClusterInfo(REGION=SYSTEM_ENV.DISCOVER_REGION, SERVICE_NAME=SYSTEM_ENV.NAME).dict()
            code, msg = request_util.post_to_system(get_cluster_url, data)
            if code != 0:
                task_result.CODE = TaskResultCode.FAIL
                task_result.ERROR_MSG = "failed to get cluster information from api server"
                return task_result
            else:
                self._cluster = msg["SERVERS"]

            if url in self._cluster:
                models = {}
                for k, v in self._loaded_models.items():
                    models[k] = {"version_state": v.states, "latest": v.latest}

                pipelines = self._loaded_ensembles

                services = []
                for infer_path, infer_def in self._routing_table_infer.items():
                    project, service_name = dissemble_service_path(infer_path)
                    model_key, version = dissemble_kserve_inference_path(infer_def.path)
                    use_seq = True
                    if infer_def.url is None:
                        use_seq = False
                    desc_path = create_desc_path(project, service_name)
                    input_desc = self._routing_table_desc[desc_path].REQUEST.DATA["inputs"]
                    output_desc_schema = self._routing_table_desc[desc_path].RESPONSE.DATA["outputs"]
                    output_schema = []
                    for _ in output_desc_schema:
                        output_schema.append({"name": _["name"], "datatype": _["datatype"], "dims": _["shape"]})
                    services.append(ServiceEndpointInfo(SVC_NM=service_name, PRJ_ID=project, MDL_KEY=model_key,
                                                        VERSION=version,
                                                        USE_SEQUENCE=use_seq, INPUT_SCHEMA=input_desc,
                                                        OUTPUT_SCHEMA=output_schema).dict())
                ifr_svc_info = InferenceServiceStateInfo(MODELS=models, PIPELINES=pipelines, SERVICES=services)
                state = InferenceServiceState(MODIFY_COUNT=self._modify_count, STATE=ifr_svc_info)
                self._logger.info(f"server '{url}' joined in cluster")
                task_result.RESULT_VALUE = state
                return task_result
            else:
                self._cluster.remove(url)
                task_result.MESSAGE = f"failed to append server '{url}' to cluster. server not registered."
                return task_result

    def load_model(self, model_info: req_vo.LoadModel) -> TaskResult:
        with self._consensus_task():
            task_result = TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
            model_key = model_info.MDL_KEY
            if self._update_config_xin(model_key):
                for _ in range(MAX_RETRY):
                    time.sleep(RETRY_DELAY)
                    if not self._update_config_xin(model_key):
                        break
                else:
                    task_result.MESSAGE = f"failed to update model config. max retry exceeded"
                    task_result.CODE = TaskResultCode.FAIL
                    return task_result
            with RollbackContext(task=self._rollback_load_model, model_key=model_key):
                if model_info.LATEST is None:
                    if model_info.MDL_KEY in self._loaded_models:
                        model_info.LATEST = self._loaded_models[model_key].latest
                    else:
                        raise ValueError(f"latest version of model is not defined. "
                                         f"latest version must be defined for first deploy")
                versions_be_loaded = []
                update_latest = False
                if model_key in self._loaded_models:
                    for version in model_info.VERSIONS:
                        if version not in self._loaded_models[model_key].states.keys():
                            versions_be_loaded.append(version)
                    if model_info.LATEST != self._loaded_models[model_key].latest:
                        update_latest = True
                    if not versions_be_loaded and not update_latest:
                        return task_result
                else:
                    versions_be_loaded = model_info.VERSIONS
                specific_versions = []
                if model_info.MDL_KEY in self._loaded_models:
                    with self._lock_loaded_models:
                        specific_versions += list(self._loaded_models[model_key].states.keys())
                specific_versions += versions_be_loaded
                specific_versions = [model_info.LATEST if x == -1 else x for x in specific_versions]
                s3_path = get_s3_path(model_key=model_key)
                version_policy = create_version_policy(specific_versions)
                try:
                    cur_config = self._rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = f"failed to read config from server"
                    return task_result
                self._update_config_state[model_key].config_backup = cur_config
                new_config = model_config_to_dict(cur_config)
                new_config["versionPolicy"] = version_policy
                new_config = dict_to_model_config(new_config)

                try:
                    self._rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config.encode(),
                                                target_path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = f"failed to upload config to server"
                    return task_result
                self._update_config_state[model_key].is_uploaded = True

                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = msg
                    return task_result
                self._update_config_state[model_key].is_loaded = True

                with self._lock_loaded_models:
                    if model_key in self._loaded_models:
                        self._update_config_state[model_key].state_backup = copy.deepcopy(
                            self._loaded_models[model_key])
                    else:
                        self._update_config_state[model_key].state_backup = None
                    for version in model_info.VERSIONS:
                        if model_key in self._loaded_models:
                            if version in self._loaded_models[model_key].states:
                                self._loaded_models[model_key].states[version] += 1
                            else:
                                self._loaded_models[model_key].states = {version: 1}
                        else:
                            self._loaded_models[model_key] = ModelVersionState(states={version: 1},
                                                                               latest=model_info.LATEST)

                    self._loaded_models[model_key].latest = model_info.LATEST

                # propagation
                prop_urls = [u + PropPath.LOAD_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    data = self._loaded_models[model_key].copy().dict()
                    prop_data = req_vo.PropLoadModel(KEY=model_key, DATA=data, VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        self._update_config_state[model_key].propagated = success
                        task_result.CODE = TaskResultCode.FAIL
                        task_result.MESSAGE = "propagation failed"
                        return task_result

                self._update_config_state[model_key].is_done = True
                return task_result

    def _rollback_load_model(self, model_key: str):
        if model_key not in self._update_config_state:
            self._logger.warning(f"failed to find {model_key} in update_config_state")
            return
        load_model_state = self._update_config_state[model_key]
        try:
            if not load_model_state.is_done:
                if load_model_state.state_backup is not None:
                    with self._lock_loaded_models:
                        self._loaded_models[model_key] = load_model_state.state_backup
                else:
                    if model_key in self._loaded_models:
                        del self._loaded_models[model_key]
                if load_model_state.is_uploaded:
                    self._rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=load_model_state.config_backup,
                                                target_path=f"{model_key}/{MODEL_CONFIG_FILENAME}")
                if load_model_state.is_loaded:
                    if load_model_state.state_backup is not None:
                        url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/load"
                        code, msg = request_util.post(url=url)
                        if code != 0:
                            self._logger.error(msg=msg)
                    else:
                        url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/unload"
                        code, msg = request_util.post(url=url)
                        if code != 0:
                            self._logger.error(msg=msg)
                if load_model_state.propagated:
                    prop_urls = [u + PropPath.ROLLBACK_LOAD_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                    if prop_urls:
                        if model_key in self._loaded_models:
                            data = self._loaded_models[model_key].copy().dict()
                        else:
                            data = None
                        prop_data = req_vo.PropLoadModel(KEY=model_key, DATA=data, VER=self._modify_count).dict()
                        propagate(urls=prop_urls, data=prop_data)

        except Exception as exc:
            self._logger.error(f"failed to execute rollback_load_model {exc.__str__()}, {traceback.format_exc()}")
        finally:
            self._remove_update_config_state(model_key=model_key)

    def prop_load_model(self, prop_data: req_vo.PropLoadModel) -> TaskResult:
        model_info: ModelVersionState = ModelVersionState(**prop_data.DATA)
        version = prop_data.VER
        if version > self._modify_count:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{prop_data.KEY}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to load model on triton")
            with self._lock_loaded_models:
                self._loaded_models[prop_data.KEY] = model_info
            self._modify_count = version
            return TaskResult(CODE=TaskResultCode.DONE)
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def prop_rollback_load_model(self, prop_data: req_vo.PropLoadModel):
        if prop_data.DATA is None:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{prop_data.KEY}/unload"
            code, msg = request_util.post(url=url)
            if code != 0:
                self._logger.error(msg=msg)
            del self._loaded_models[prop_data.KEY]
        else:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{prop_data.KEY}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                self._logger.error(msg=msg)
            self._loaded_models[prop_data.KEY] = ModelVersionState(**prop_data.DATA)

    def unload_model(self, model_info: req_vo.UnloadModel):
        task_result = TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        model_key = model_info.MDL_KEY
        with self._consensus_task():
            if self._update_config_xin(model_key):
                for _ in range(MAX_RETRY):
                    time.sleep(RETRY_DELAY)
                    if not self._update_config_xin(model_key):
                        break
                else:
                    raise RuntimeError(f"failed to update model config. max retry exceeded")
            self._update_config_state[model_key].is_update_config_set = True
            with RollbackContext(task=self._rollback_load_model, model_key=model_key):
                if model_key not in self._loaded_models:
                    return task_result

                self._update_config_state[model_key].state_backup = copy.deepcopy(self._loaded_models[model_key])
                loaded_version_state = self._loaded_models[model_key].states.copy()
                for version in model_info.VERSIONS:
                    if version in loaded_version_state:
                        loaded_version_state[version] -= 1
                        if loaded_version_state[version] <= 0:
                            del loaded_version_state[version]
                latest = self._loaded_models[model_key].latest
                with self._lock_loaded_models:
                    cur_specific_versions = list(self._loaded_models[model_key].states.keys())
                cur_specific_versions = [latest if x == -1 else x for x in cur_specific_versions]
                new_specific_versions = list(loaded_version_state.keys())
                new_specific_versions = [latest if x == -1 else x for x in new_specific_versions]

                with self._lock_loaded_models:
                    if set(new_specific_versions) == set(cur_specific_versions):
                        self._loaded_models[model_key].states = loaded_version_state

                        # prop
                        prop_urls = [u + PropPath.UNLOAD_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                        if prop_urls:
                            data = self._loaded_models[model_key].copy().dict()
                            prop_data = req_vo.PropUnloadModel(IS_TRITON_UPDATED=False, KEY=model_key,
                                                               DATA=data, VER=self._modify_count).dict()
                            success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                            if len(success) < len(prop_urls):
                                self._update_config_state[model_key].propagated = success
                                task_result.CODE = TaskResultCode.FAIL
                                task_result.MESSAGE = "failed to propagation"
                                return task_result

                        self._update_config_state[model_key].is_done = True
                        return task_result

                if not set(new_specific_versions):
                    url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.MDL_KEY}/unload"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        task_result.CODE = TaskResultCode.FAIL
                        task_result.MESSAGE = msg
                        return task_result
                    else:
                        self._update_config_state[model_key].is_loaded = True

                    with self._lock_loaded_models:
                        del self._loaded_models[model_key]

                    # prop
                    prop_urls = [u + PropPath.UNLOAD_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                    if prop_urls:
                        prop_data = req_vo.PropUnloadModel(IS_TRITON_UPDATED=True, KEY=model_key,
                                                           VER=self._modify_count, IS_DELETED=True).dict()
                        success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                        if len(success) < len(prop_urls):
                            self._update_config_state[model_key].propagated = success
                            task_result.CODE = TaskResultCode.FAIL
                            task_result.MESSAGE = "failed to propagation"
                            return task_result

                    self._update_config_state[model_key].is_done = True
                    return task_result

                version_policy = create_version_policy(list(set(new_specific_versions)))
                s3_path = get_s3_path(model_key=model_key)
                try:
                    cur_config = self._rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = f"failed to read config from server"
                    return task_result
                self._update_config_state[model_key].config_backup = cur_config
                new_config = model_config_to_dict(cur_config)
                new_config["versionPolicy"] = version_policy
                new_config = dict_to_model_config(new_config)
                try:
                    self._rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config, target_path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = f"failed to upload config to server"
                    return task_result
                self._update_config_state[model_key].is_uploaded = True

                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    task_result.CODE = TaskResultCode.FAIL
                    task_result.MESSAGE = msg
                    return task_result
                else:
                    self._update_config_state[model_key].is_loaded = True

                with self._lock_loaded_models:
                    self._loaded_models[model_key].states = loaded_version_state

                # prop
                prop_urls = [u + PropPath.UNLOAD_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    data = self._loaded_models[model_key].copy().dict()
                    prop_data = req_vo.PropUnloadModel(IS_TRITON_UPDATED=True, KEY=model_key, DATA=data,
                                                       VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        self._update_config_state[model_key].propagated = success
                        task_result.CODE = TaskResultCode.FAIL
                        task_result.MESSAGE = "failed to propagation"
                        return task_result

                self._update_config_state[model_key].is_done = True
                return task_result

    def prop_unload_model(self, unload_info: req_vo.PropUnloadModel):
        if unload_info.VER > self._modify_count:
            if unload_info.IS_DELETED:
                with self._lock_loaded_models:
                    url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{unload_info.KEY}/unload"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to unload model on triton")
                    del self._loaded_models[unload_info.KEY]
                    self._modify_count = unload_info.VER
                    return TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
            if unload_info.IS_TRITON_UPDATED:
                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{unload_info.KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to unload model on triton")
            model_info: ModelVersionState = ModelVersionState(**unload_info.DATA)
            with self._loaded_models:
                self._loaded_models[unload_info.KEY] = model_info
            self._modify_count = unload_info.VER
            return TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def load_ensemble(self, model_info: req_vo.LoadEnsemble) -> TaskResult:
        with self._consensus_task():
            self._update_ensemble_state[model_info.MDL_KEY] = LoadEnsembleState()
            update_state = self._update_ensemble_state[model_info.MDL_KEY]
            with RollbackContext(task=self._rollback_load_ensemble, model_key=model_info.MDL_KEY):
                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to load model on triton")
                update_state.is_loaded = True
                self._loaded_ensembles.append(model_info.MDL_KEY)
                update_state.is_set = True
                # prop
                prop_urls = [u + PropPath.LOAD_ENSEMBLE for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropLoadEnsemble(KEY=model_info.MDL_KEY, VER=self._modify_count,
                                                        PIPES=self._loaded_ensembles).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        update_state.propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                update_state.is_done = True
                return TaskResult(CODE=TaskResultCode.DONE, MESSAGE="")

    def _rollback_load_ensemble(self, model_key: str):
        if model_key not in self._update_ensemble_state:
            return
        try:
            progress = self._update_ensemble_state[model_key]
            if not progress.is_done:
                if progress.is_loaded:
                    url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/unload"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        self._logger.error(msg)
                if progress.is_unloaded:
                    url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/load"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        self._logger.error(msg)
                if progress.is_set:
                    self._loaded_ensembles.remove(model_key)
                if progress.is_del:
                    self._loaded_ensembles.append(model_key)
                if progress.propagated:
                    prop_urls = [u + PropPath.ROLLBACK_LOAD_ENSEMBLE for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                    if prop_urls:
                        prop_data = req_vo.PropLoadEnsemble(KEY=model_key, VER=self._modify_count,
                                                            PIPES=self._loaded_ensembles).dict()
                        propagate(urls=prop_urls, data=prop_data)
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
        finally:
            del self._update_ensemble_state[model_key]

    def prop_load_ensemble(self, model_info: req_vo.PropLoadEnsemble) -> TaskResult:
        if model_info.VER > self._modify_count:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.KEY}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to load model on triton")
            self._loaded_ensembles.append(model_info.KEY)
            return TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def prop_rollback_load_ensemble(self, model_info: req_vo.PropLoadEnsemble):
        loaded = copy.deepcopy(self._loaded_ensembles)
        for pipe in model_info.PIPES:
            if pipe not in self._loaded_ensembles:
                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{pipe}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    self._logger.error(msg)
                self._loaded_ensembles.append(pipe)
            else:
                loaded.remove(pipe)

        for pipe in loaded:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{pipe}/unload"
            code, msg = request_util.post(url=url)
            if code != 0:
                self._logger.error(msg)
            self._loaded_ensembles.remove(pipe)

    def unload_ensemble(self, model_info: req_vo.LoadEnsemble):
        with self._consensus_task():
            self._update_ensemble_state[model_info.MDL_KEY] = LoadEnsembleState()
            update_state = self._update_ensemble_state[model_info.MDL_KEY]
            with RollbackContext(task=self._rollback_load_ensemble, model_key=model_info.MDL_KEY):
                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.MDL_KEY}/unload"
                code, msg = request_util.post(url=url)
                if code != 0:
                    return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE=f"failed to unload model on triton. {msg}")
                update_state.is_unloaded = True
                self._loaded_ensembles.remove(model_info.MDL_KEY)
                update_state.is_del = True
                # prop
                prop_urls = [u + PropPath.UNLOAD_ENSEMBLE for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropLoadEnsemble(KEY=model_info.MDL_KEY, VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        update_state.propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                update_state.is_done = True
                return TaskResult(CODE=TaskResultCode.DONE, MESSAGE="")

    def prop_unload_ensemble(self, model_info: req_vo.PropLoadEnsemble) -> TaskResult:
        if model_info.VER > self._modify_count:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_info.KEY}/unload"
            code, msg = request_util.post(url=url)
            if code != 0:
                return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to unload model on triton")
            self._loaded_ensembles.append(model_info.KEY)
            return TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def create_endpoint(self, req_body: req_vo.CreateEndpoint) -> TaskResult:
        result = TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        with self._consensus_task():
            path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
            self._create_routing_state[path_infer] = CreateRoutingState()
            update_state = self._create_routing_state[path_infer]
            with RollbackContext(task=self._rollback_create_endpoint, rout_key=path_infer):
                with self._lock_routing_table_infer:
                    if path_infer in self._routing_table_infer:
                        update_state.is_done = True
                        return result

                    path = create_kserve_inference_path(model_name=req_body.MDL_KEY, version=req_body.VERSION)
                    infer_schema = []
                    for inference_io in req_body.INPUT_SCHEMA:
                        infer_schema.append((inference_io.name, DATATYPE_MAP[inference_io.datatype]))
                    if req_body.USE_SEQUENCE:
                        url = get_suitable_server() + path
                        inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema, url=url)
                        self._app.add_api_route(path_infer, self._make_inference_sequence, methods=["POST"])
                    else:
                        inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema)
                        self._app.add_api_route(path_infer, self._make_inference, methods=["POST"])
                    self._routing_table_infer[path_infer] = inference_service_definition
                    update_state.infer_path = path_infer
                with self._lock_routing_table_desc:
                    inference_service_description = create_service_description(url=SYSTEM_ENV.DISCOVER_URL + path_infer,
                                                                               input_desc=create_input_description(
                                                                                   req_body.INPUT_SCHEMA),
                                                                               output_desc=create_output_description(
                                                                                   req_body.OUTPUT_SCHEMA))
                    self._app.add_api_route(path_desc, self._get_service_description, methods=["GET"],
                                            response_model=res_vo.ServiceDescribe)
                    self._routing_table_desc[path_desc] = inference_service_description
                    update_state.desc_path = path_desc

                # prop
                prop_urls = [u + PropPath.CREATE_ENDPOINT for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropCreateEndpoint(INFER_KEY=path_infer,
                                                          INFER_VAL=asdict(inference_service_definition),
                                                          DESC_KEY=path_desc,
                                                          DESC_VAL=inference_service_description.dict(),
                                                          VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        update_state.propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                update_state.is_done = True
                return result

    def _rollback_create_endpoint(self, rout_key: str):
        if rout_key not in self._create_routing_state:
            return
        progress = self._create_routing_state[rout_key]
        try:
            if not progress.is_done:
                if progress.infer_path is not None:
                    del self._routing_table_infer[progress.infer_path]
                    self.remove_router(progress.infer_path)
                if progress.desc_path is not None:
                    del self._routing_table_desc[progress.desc_path]
                    self.remove_router(progress.desc_path)
                if progress.propagated:
                    self._overwrite_endpoint()
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
        finally:
            del self._create_routing_state[rout_key]

    def _overwrite_endpoint(self):
        prop_urls = [u + PropPath.OVERWRITE_ENDPOINT for u in self._cluster]
        ep_infer = {}
        for k, v in self._routing_table_infer.items():
            ep_infer[k] = asdict(v)
        ep_desc = {}
        for k, v in self._routing_table_desc.items():
            ep_desc[k] = v.dict()
        prop_data = req_vo.OverwriteEndpoints(EP_INFER=ep_infer,
                                              EP_DESC=ep_desc).dict()
        if prop_urls:
            propagate(urls=prop_urls, data=prop_data)

    def prop_overwrite_endpoint(self, req_body: req_vo.OverwriteEndpoints):
        loaded = copy.deepcopy(self._routing_table_infer)
        for k, v in req_body.EP_INFER.items():
            if k not in loaded:
                self._routing_table_infer[k] = InferenceServiceDef(**v)
                if self._routing_table_infer[k].url is None:
                    self._app.add_api_route(k, self._make_inference, methods=["POST"])
                else:
                    self._app.add_api_route(k, self._make_inference_sequence, methods=["POST"])
            else:
                del loaded[k]
        for _ in loaded:
            self.remove_router(_)

        loaded = copy.deepcopy(self._routing_table_desc)
        for k, v in req_body.EP_DESC.items():
            if k not in loaded:
                self._routing_table_desc[k] = ServiceDescription(**v)
                self._app.add_api_route(k, self._get_service_description, methods=["GET"],
                                        response_model=res_vo.ServiceDescribe)
            else:
                del loaded[k]
        for _ in loaded:
            self.remove_router(_)

    def prop_create_endpoint(self, routing_info: req_vo.PropCreateEndpoint) -> TaskResult:
        if routing_info.VER > self._modify_count:
            self._routing_table_desc[routing_info.DESC_KEY] = ServiceDescription(**routing_info.DESC_VAL)
            self._app.add_api_route(routing_info.DESC_KEY, self._get_service_description, methods=["GET"],
                                    response_model=res_vo.ServiceDescribe)

            self._routing_table_infer[routing_info.INFER_KEY] = InferenceServiceDef(**routing_info.INFER_VAL)
            if self._routing_table_infer[routing_info.INFER_KEY].url is None:
                self._app.add_api_route(routing_info.INFER_KEY, self._make_inference, methods=["POST"])
            else:
                self._app.add_api_route(routing_info.INFER_KEY, self._make_inference_sequence, methods=["POST"])
            self._modify_count = routing_info.VER
            return TaskResult(CODE=TaskResultCode.DONE)
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def remove_endpoint(self, req_body: req_vo.RemoveEndpoint) -> TaskResult:
        result_msg = TaskResult(CODE=TaskResultCode.DONE, MESSAGE='')
        with self._consensus_task():
            path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
            self._delete_routing_state[path_infer] = DeleteRoutingState()
            delete_state = self._delete_routing_state[path_infer]
            with RollbackContext(task=self._rollback_remove_endpoint, route_key=path_infer):
                with self._lock_routing_table_infer:
                    if path_infer in self._routing_table_infer:
                        self.remove_router(path_infer)
                        delete_state.infer_val_backup = copy.deepcopy(self._routing_table_infer[path_infer])
                        delete_state.infer_key = path_infer
                        del self._routing_table_infer[path_infer]
                        delete_state.is_route_infer_deleted = True
                with self._lock_routing_table_desc:
                    if path_desc in self._routing_table_desc:
                        self.remove_router(path_desc)
                        delete_state.desc_val_backup = copy.deepcopy(self._routing_table_desc[path_desc])
                        delete_state.desc_key = path_desc
                        del self._routing_table_desc[path_desc]
                        delete_state.is_route_desc_deleted = True
                # prop
                prop_urls = [u + PropPath.REMOVE_ENDPOINT for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropRemoveEndpoint(PRJ_ID=req_body.PRJ_ID,
                                                          SVC_NM=req_body.SVC_NM,
                                                          VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        delete_state.propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                delete_state.is_done = True
                return result_msg

    def _rollback_remove_endpoint(self, route_key: str):
        if route_key not in self._delete_routing_state:
            return
        progress = self._delete_routing_state[route_key]
        try:
            if not progress.is_done:
                if progress.is_route_desc_deleted:
                    self._routing_table_desc[progress.desc_key] = progress.desc_val_backup
                    self._app.add_api_route(progress.desc_key, self._get_service_description, methods=["GET"],
                                            response_model=res_vo.ServiceDescribe)
                if progress.is_route_infer_deleted:
                    self._routing_table_infer[progress.infer_key] = progress.infer_val_backup
                    if self._routing_table_infer[progress.infer_key].url is None:
                        self._app.add_api_route(progress.infer_key, self._make_inference, methods=["POST"])
                    else:
                        self._app.add_api_route(progress.infer_key, self._make_inference_sequence, methods=["POST"])
                if progress.propagated:
                    self._overwrite_endpoint()
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
        finally:
            del self._delete_routing_state[route_key]

    def prop_remove_endpoint(self, req_body: req_vo.PropRemoveEndpoint) -> TaskResult:
        if req_body.VER <= self._modify_count:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")
        path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
        with self._lock_routing_table_infer:
            if path_infer in self._routing_table_infer:
                self.remove_router(path_infer)
                del self._routing_table_infer[path_infer]
        with self._lock_routing_table_desc:
            if path_desc in self._routing_table_desc:
                self.remove_router(path_desc)
                del self._routing_table_desc[path_desc]
        return TaskResult(CODE=TaskResultCode.DONE)

    def update_endpoint(self, req_body: req_vo.UpdateEndpoint) -> TaskResult:
        result_msg = TaskResult(CODE=TaskResultCode.DONE)
        path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
        if path_infer in self._routing_table_infer and path_desc in self._routing_table_desc:
            self._update_routing_state[path_infer] = UpdateRoutingState()
            update_state = self._update_routing_state[path_infer]
            with RollbackContext(task=self._rollback_update_endpoint, route_key=path_infer):
                with self._lock_routing_table_infer:
                    path = create_kserve_inference_path(model_name=req_body.MDL_KEY, version=req_body.VERSION)
                    infer_schema = []
                    for inference_io in req_body.INPUT_SCHEMA:
                        infer_schema.append((inference_io.name, DATATYPE_MAP[inference_io.datatype]))
                    if req_body.USE_SEQUENCE:
                        url = get_suitable_server() + path
                        inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema, url=url)
                    else:
                        inference_service_definition = InferenceServiceDef(path=path, schema_=infer_schema)
                    update_state.infer_key = path_infer
                    update_state.infer_val_backup = copy.deepcopy(self._routing_table_infer[path_infer])
                    self._routing_table_infer[path_infer] = inference_service_definition
                    update_state.is_route_infer_changed = True
                with self._lock_routing_table_desc:
                    inference_service_description = create_service_description(url=SYSTEM_ENV.DISCOVER_URL + path_infer,
                                                                               input_desc=create_input_description(
                                                                                   req_body.INPUT_SCHEMA),
                                                                               output_desc=create_output_description(
                                                                                   req_body.OUTPUT_SCHEMA))
                    update_state.desc_key = path_desc
                    update_state.desc_val_backup = copy.deepcopy(self._routing_table_desc[path_desc])
                    self._routing_table_desc[path_desc] = inference_service_description
                    update_state.is_route_desc_changed = True
                # prop
                prop_urls = [u + PropPath.UPDATE_ENDPOINT for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropCreateEndpoint(INFER_KEY=path_infer,
                                                          INFER_VAL=asdict(inference_service_definition),
                                                          DESC_KEY=path_desc,
                                                          DESC_VAL=inference_service_description.dict(),
                                                          VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < len(prop_urls):
                        update_state.propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                update_state.is_done = True
                return result_msg
        else:
            result_msg.MESSAGE = f"service {req_body.SVC_NM} not exist."
            result_msg.CODE = TaskResultCode.FAIL
            return result_msg

    def _rollback_update_endpoint(self, route_key: str):
        if route_key not in self._update_routing_state:
            return
        progress = self._update_routing_state[route_key]
        try:
            if not progress.is_done:
                if progress.is_route_desc_changed:
                    self._routing_table_desc[progress.desc_key] = progress.desc_val_backup
                if progress.is_route_infer_changed:
                    self._routing_table_infer[progress.infer_key] = progress.infer_val_backup
                if progress.propagated:
                    self._overwrite_endpoint()
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
        finally:
            del self._update_routing_state[route_key]

    def prop_update_endpoint(self, req_body: req_vo.PropCreateEndpoint) -> TaskResult:
        if req_body.VER > self._modify_count:
            with self._lock_routing_table_infer:
                self._routing_table_infer[req_body.INFER_KEY] = InferenceServiceDef(**req_body.INFER_VAL)
            with self._lock_routing_table_desc:
                self._routing_table_desc[req_body.DESC_KEY] = ServiceDescription(**req_body.DESC_VAL)
            return TaskResult(CODE=TaskResultCode.DONE)
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def update_model_latest(self, req_body: req_vo.ModelLatest) -> TaskResult:
        result_msg = TaskResult(CODE=TaskResultCode.DONE)
        if self._update_config_xin(req_body.MDL_KEY):
            for _ in range(MAX_RETRY):
                time.sleep(RETRY_DELAY)
                if not self._update_config_xin(req_body.MDL_KEY):
                    break
            else:
                result_msg.MESSAGE = f"failed to update model config. max retry exceeded"
                result_msg.CODE = TaskResultCode.FAIL
                return result_msg
        with RollbackContext(task=self.rollback_update_model_latest, model_key=req_body.MDL_KEY):
            if req_body.MDL_KEY in self._loaded_models:
                specific_versions = list(self._loaded_models[req_body.MDL_KEY].states.keys())
                specific_versions = [req_body.LATEST_VER if x == -1 else x for x in specific_versions]
                s3_path = get_s3_path(model_key=req_body.MDL_KEY)
                version_policy = create_version_policy(specific_versions)
                try:
                    cur_config = self._rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    result_msg.CODE = TaskResultCode.FAIL
                    result_msg.MESSAGE = f"failed to read config from server"
                    return result_msg
                with self._lock_update_config:
                    self._update_config_state[req_body.MDL_KEY].config_backup = cur_config
                new_config = model_config_to_dict(cur_config)
                new_config["versionPolicy"] = version_policy
                new_config = dict_to_model_config(new_config)
                try:
                    self._rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config.encode(),
                                                target_path=s3_path)
                except Exception as exc:
                    self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                    result_msg.CODE = RequestResult.FAIL
                    result_msg.ERROR_MSG = f"failed to upload config to server"
                    return result_msg
                with self._lock_update_config:
                    self._update_config_state[req_body.MDL_KEY].is_uploaded = True

                url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    result_msg.CODE = RequestResult.FAIL
                    result_msg.ERROR_MSG = msg
                    return result_msg
                with self._lock_loaded_models:
                    self._update_config_state[req_body.MDL_KEY].state_backup = copy.deepcopy(
                        self._loaded_models[req_body.MDL_KEY])
                    self._loaded_models[req_body.MDL_KEY].latest = req_body.LATEST_VER
                with self._lock_update_config:
                    self._update_config_state[req_body.MDL_KEY].is_loaded = True

                # prop
                prop_urls = [u + PropPath.UPDATE_MODEL for u in self._cluster if u != SYSTEM_ENV.DISCOVER_URL]
                if prop_urls:
                    prop_data = req_vo.PropUpdateModelLatest(KEY=req_body.MDL_KEY,
                                                             STATE=self._loaded_models[req_body.MDL_KEY].copy().dict(),
                                                             VER=self._modify_count).dict()
                    success = propagate(urls=prop_urls, data=prop_data, ignore_error=True)
                    if len(success) < prop_urls:
                        self._update_config_state[req_body.MDL_KEY].propagated = success
                        return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="failed to propagation")

                self._update_config_state[req_body.MDL_KEY].is_done = True
            else:
                self._update_config_state[req_body.MDL_KEY].is_done = True
            return result_msg

    def rollback_update_model_latest(self, model_key: str):
        if model_key not in self._update_config_state:
            self._logger.warning(f"failed to find {model_key} in update_config_state")
            return
        load_model_state = self._update_config_state[model_key]
        try:
            if not load_model_state.is_done:
                if load_model_state.is_uploaded:
                    try:
                        self._rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=load_model_state.config_backup,
                                                    target_path=f"{model_key}/{MODEL_CONFIG_FILENAME}")
                    except Exception as exc:
                        self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                if load_model_state.is_loaded:
                    url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/load"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        self._logger.error(msg=msg)
                if load_model_state.state_backup is not None:
                    self._loaded_models[model_key] = load_model_state.state_backup
                if load_model_state.propagated:
                    prop_urls = [u + PropPath.ROLLBACK_UPDATE_MODEL for u in self._cluster]
                    prop_data = req_vo.PropUpdateModelLatest(KEY=model_key,
                                                             STATE=self._loaded_models[model_key].copy().dict(),
                                                             VER=self._modify_count).dict()
                    if prop_urls:
                        propagate(urls=prop_urls, data=prop_data)
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
        finally:
            self._remove_update_config_state(model_key=model_key)

    def prop_update_model_latest(self, req_body: req_vo.PropUpdateModelLatest) -> TaskResult:
        if req_body.VER > self._modify_count:
            url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.KEY}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE=msg)
            self._loaded_models[req_body.KEY] = ModelVersionState(**req_body.STATE)
            self._modify_count = req_body.VER
            return TaskResult(CODE=TaskResultCode.DONE)
        else:
            return TaskResult(CODE=TaskResultCode.FAIL, MESSAGE="the version of data is out of date")

    def overwrite_model_state(self, req_body: req_vo.OverwriteModelState):
        url = SYSTEM_ENV.TRITON_SERVER_URL + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.KEY}/load"
        code, msg = request_util.post(url=url)
        if code != 0:
            self._logger.error(msg)
        self._loaded_models[req_body.KEY] = ModelVersionState(**req_body.STATE)
        return TaskResult(CODE=TaskResultCode.DONE)

    def _update_config_xin(self, model_key: str):
        with self._lock_update_config:
            if model_key in self._update_config_state:
                return True
            else:
                self._update_config_state[model_key] = LoadModelState()
                return False

    def _remove_update_config_state(self, model_key: str):
        with self._lock_update_config:
            if model_key in self._update_config_state:
                del self._update_config_state[model_key]

    async def _make_inference(self, request: Request, req_body: req_vo.InferenceInput,
                              background_tasks: BackgroundTasks):
        path = request.url.path
        try:
            url, schema = self._get_inference_url_schema(key=path)
        except Exception as exc:
            self._logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
            return JSONResponse(status_code=400, content={"error": "failed to find service"})
        inputs = make_inference_input(schema=schema, inputs=req_body.inputs)
        code, msg = request_util.post(url=url, data=inputs)
        if code != 0:
            return JSONResponse(status_code=400, content=json.loads(msg))
        else:
            return {"outputs": msg["outputs"]}

    async def _make_inference_sequence(self, request: Request, req_body: req_vo.InferenceInput,
                                       background_tasks: BackgroundTasks):
        path = request.url.path
        url, schema = self._get_inference_url_schema_sequence(key=path)
        inputs = make_inference_input(schema=schema, inputs=req_body.inputs)
        code, msg = request_util.post(url=url, data=inputs)
        if code != 0:
            return JSONResponse(status_code=400, content=json.loads(msg))
        else:
            return {"outputs": msg["outputs"]}

    def _get_inference_url_schema(self, key: str) -> tuple[str, list[tuple[str, str]]]:
        inference_service_def = self._routing_table_infer[key]
        url = SYSTEM_ENV.TRITON_SERVER_URL + inference_service_def.path
        return url, inference_service_def.schema_

    def _get_inference_url_schema_sequence(self, key: str) -> tuple[str, list[tuple[str, str]]]:
        inference_service_def = self._routing_table_infer[key]
        return inference_service_def.url, inference_service_def.schema_

    async def _get_service_description(self, request: Request):
        result_msg = res_vo.ServiceDescribe(CODE=RequestResult.SUCCESS, ERROR_MSG='', SERVICE_DESC={})
        path = request.url.path
        if path in self._routing_table_desc:
            service_description = self._routing_table_desc[path]
            result_msg.SERVICE_DESC = service_description.dict()
            return result_msg
        else:
            result_msg.CODE = RequestResult.FAIL
            result_msg.ERROR_MSG = "service not exist"
            return result_msg

    def remove_router(self, path: str):
        for idx, router in enumerate(self._app.routes):
            if router.path_format == path:
                del self._app.routes[idx]
                break
        else:
            raise RuntimeError(f"failed to remove router {path}. doesn't exist")

    @property
    def routing_table_desc(self):
        return self._routing_table_desc

    @property
    def loaded_models(self):
        return self._loaded_models


def get_suitable_server() -> str:
    return SYSTEM_ENV.TRITON_SERVER_URL


def get_agreement(url) -> tuple[int, str]:
    url_get_agreement = f"{url + PropPath.ACQUIRE_TASK_HANDLE}?url={SYSTEM_ENV.DISCOVER_URL}"
    code, msg = request_util.get_from_system(url_get_agreement)
    if code == 0:
        return msg["AGREEMENT"], url
    else:
        raise RuntimeError(msg)


def prop_request(request_info: tuple):
    url = request_info[0]
    data = request_info[1]
    if data is None:
        return url, request_util.get_from_system(url)
    else:
        return url, request_util.post_to_system(url=url, data=data)


def propagate(urls: list[str], data: dict | None = None, rollback_path: str | None = None,
              rollback_data: dict | None = None, ignore_error: bool = False):
    request_info = []
    failed = []
    success = []
    rollback_info = []
    for url in urls:
        request_info.append((url, data))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = list(executor.map(prop_request, request_info))
    for url, code, msg in result:
        if code != 0:
            failed.append((url, msg))
        else:
            success.append(url)
    if rollback_path is not None:
        for url, msg in failed:
            rollback_info.append((url + rollback_path, rollback_data))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(prop_request, rollback_info)
    if ignore_error:
        if failed:
            raise RuntimeError(f"failed to propagate '{failed}'")
    else:
        return success
