import json
import logging
import time
import traceback
from multiprocessing import Lock
from itertools import cycle

from fastapi import APIRouter, Depends, Request, HTTPException, BackgroundTasks, FastAPI
from fastapi.responses import JSONResponse
import pydantic
from cryptography.fernet import Fernet

import request_util
import request_vo as req_vo
import response_vo as res_vo
from rw_util import RWUtil
from utils import (RollbackContext, create_version_policy, dict_to_model_config, model_config_to_dict, get_s3_path,
                   make_inference_input, create_service_path, create_kserve_inference_path, create_service_description)
from _types import RequestResult, ModelVersionState, LoadModelState, InferenceServiceDef, ServiceDescription, \
    DATATYPE_MAP
from _constants import KEY, ORIGIN, SYSTEM_ENV, RequestPath, MAX_RETRY, RETRY_DELAY, MODEL_CONFIG_FILENAME, ModelStore


async def validator_load_model(request: Request) -> req_vo.LoadModel:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.LoadModel(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


async def validator_unload_model(request: Request) -> req_vo.UnloadModel:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.UnloadModel(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


async def validator_load_ensemble(request: Request) -> req_vo.LoadEnsemble:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.LoadEnsemble(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


async def validator_create_endpoint(request: Request) -> req_vo.CreateEndpoint:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.CreateEndpoint(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


async def validator_remove_endpoint(request: Request) -> req_vo.RemoveEndpoint:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.RemoveEndpoint(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


def validate_header(headers):
    try:
        token = headers["Authorization"]
        token_type, _, token = token.partition(' ')
        if token_type != "Bearer":
            HTTPException(status_code=401, detail="Unauthorized")
    except KeyError:
        raise HTTPException(status_code=401, detail="Unauthorized")
    else:
        if not is_valid_token(token):
            raise HTTPException(status_code=401, detail="Unauthorized")


def is_valid_token(token: str) -> bool:
    try:
        decoded = Fernet(KEY).decrypt(token.encode()).decode()
    except Exception:
        return False
    else:
        if ORIGIN == decoded:
            return True
        else:
            return False


class InferenceRouter:
    def __init__(self, app: FastAPI, endpoints: dict | None = None):
        self._app = app
        if endpoints is not None:
            self._endpoints: dict[str, dict[str, str]] = endpoints
        else:
            self._endpoints = {}
        self.rw_util = RWUtil()

        self._cycle_triton_server = cycle(SYSTEM_ENV.TRITON_SERVER_URLS)

        self._lock_loaded_models: Lock = Lock()
        self._loaded_models: dict[str, ModelVersionState] = {}

        self._lock_update_config: Lock = Lock()
        self._update_config_state: dict[str, LoadModelState] = {}

        self._lock_routing_table_infer: Lock = Lock()
        self._routing_table_infer: dict[str, InferenceServiceDef] = {}

        self._lock_routing_table_desc: Lock = Lock()
        self._routing_table_desc: dict[str, ServiceDescription] = {}

        self.router = APIRouter()
        self.logger = logging.getLogger("root")
        self.router.add_api_route("/status/loaded_models", self.get_loaded_models, methods=["GET"], response_model=res_vo.LoadedModels)
        self.router.add_api_route("/model/load", self.load_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/model/unload", self.unload_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/ensemble/load", self.load_ensemble, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/ensemble/unload", self.unload_ensemble, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/endpoint/create", self.create_endpoint, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/endpoint/remove", self.remove_endpoint, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/test", self.test, methods=["GET"])

    def test(self):
        print(self._routing_table_infer, self._routing_table_desc)

    def get_loaded_models(self):
        result_msg = res_vo.LoadedModels(CODE=RequestResult.SUCCESS, ERROR_MSG='', LOADED_MODELS={})
        loaded_models = self._loaded_models
        result_loaded_models = {}
        for model_key, version_state in loaded_models.items():
            result_loaded_models[model_key] = {"VERSIONS": [{"VERSION": version, "REF": ref} for version, ref in version_state.states.items()], "LATEST": version_state.latest}
        result_msg.LOADED_MODELS = result_loaded_models
        return result_msg

    def create_endpoint(self, req_body: req_vo.CreateEndpoint = Depends(validator_create_endpoint)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
        with self._lock_routing_table_infer:
            if path_infer in self._routing_table_infer:
                result_msg.ERROR_MSG = f"endpoint '{req_body.EP_ID}' already exist"
                return result_msg

            path = create_kserve_inference_path(model_name=req_body.MDL_KEY, version=req_body.VERSION)
            infer_schema = []
            for inference_io in req_body.INPUT_SCHEMA:
                infer_schema.append((inference_io.name, DATATYPE_MAP[inference_io.datatype]))
            if req_body.USE_SEQUENCE:
                url = self.get_suitable_server() + path
                inference_service_definition = InferenceServiceDef(path=path, schema=infer_schema, url=url)
                self._app.add_api_route(path_infer, self.make_inference_sequence, methods=["POST"])
            else:
                inference_service_definition = InferenceServiceDef(path=path, schema=infer_schema)
                self._app.add_api_route(path_infer, self.make_inference, methods=["POST"])
            self._routing_table_infer[path_infer] = inference_service_definition
        with self._lock_routing_table_desc:
            inference_service_description = create_service_description(url=SYSTEM_ENV.DISCOVER_URL + path_infer,
                                                                       input_schema=req_body.INPUT_SCHEMA,
                                                                       output_schema=req_body.OUTPUT_SCHEMA)
            self._app.add_api_route(path_desc, self._get_service_description, methods=["GET"], response_model=res_vo.ServiceDescribe)
            self._routing_table_desc[path_desc] = inference_service_description
        return result_msg

    def remove_router(self, path: str):
        for idx, router in enumerate(self._app.routes):
            if router.path_format == path:
                del self._app.routes[idx]
                break
        else:
            raise RuntimeError(f"failed to remove router {path}. doesn't exist")

    def remove_endpoint(self, req_body: req_vo.RemoveEndpoint = Depends(validator_remove_endpoint)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        path_infer, path_desc = create_service_path(project=req_body.PRJ_ID, name=req_body.SVC_NM)
        with self._lock_routing_table_infer:
            if path_infer in self._routing_table_infer:
                self.remove_router(path_infer)
                del self._routing_table_infer[path_infer]
        with self._lock_routing_table_desc:
            if path_desc in self._routing_table_desc:
                self.remove_router(path_desc)
                del self._routing_table_desc[path_desc]
        return result_msg

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

    def _get_inference_url_schema(self, key: str) -> tuple[str, list[tuple[str, str]]]:
        inference_service_def = self._routing_table_infer[key]
        url = next(self._cycle_triton_server) + inference_service_def.path
        return url, inference_service_def.schema

    def _get_inference_url_schema_sequence(self, key: str) -> tuple[str, list[tuple[str, str]]]:
        inference_service_def = self._routing_table_infer[key]
        return inference_service_def.url, inference_service_def.schema

    async def make_inference(self, request: Request, req_body: req_vo.InferenceInput, background_tasks: BackgroundTasks):
        path = request.url.path
        try:
            url, schema = self._get_inference_url_schema(key=path)
        except Exception as exc:
            self.logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
            return JSONResponse(status_code=400, content={"error": "failed to find service"})
        inputs = make_inference_input(schema=schema, inputs=req_body.inputs)
        code, msg = request_util.post(url=url, data=inputs)
        if code != 0:
            return JSONResponse(status_code=400, content=json.loads(msg))
        else:
            return {"outputs": msg["outputs"]}

    async def make_inference_sequence(self, request: Request, req_body: req_vo.InferenceInput, background_tasks: BackgroundTasks):
        path = request.url.path
        url, schema = self._get_inference_url_schema_sequence(key=path)
        inputs = make_inference_input(schema=schema, inputs=req_body.inputs)
        code, msg = request_util.post(url=url, data=inputs)
        if code != 0:
            return JSONResponse(status_code=400, content=json.loads(msg))
        else:
            return {"outputs": msg["outputs"]}

    def load_ensemble(self, req_body: req_vo.LoadEnsemble = Depends(validator_load_ensemble)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        applied_servers = []
        for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
            url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                for loaded_server in applied_servers:
                    rollback_url = loaded_server + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/unload"
                    code, msg = request_util.post(url=rollback_url)
                    if code != 0:
                        self.logger.error(msg=msg)
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = msg
                return
            else:
                applied_servers.append(triton_url)
        return result_msg

    def unload_ensemble(self, req_body: req_vo.LoadEnsemble = Depends(validator_load_ensemble)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        applied_servers = []
        for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
            url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/unload"
            code, msg = request_util.post(url=url)
            if code != 0:
                for loaded_server in applied_servers:
                    rollback_url = loaded_server + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/load"
                    code, msg = request_util.post(url=rollback_url)
                    if code != 0:
                        self.logger.error(msg=msg)
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = msg
                return
            else:
                applied_servers.append(triton_url)
        return result_msg

    def load_model(self, req_body: req_vo.LoadModel = Depends(validator_load_model)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        model_key = req_body.MDL_KEY
        if self.update_config_xin(model_key):
            for _ in range(MAX_RETRY):
                time.sleep(RETRY_DELAY)
                if not self.update_config_xin(model_key):
                    break
            else:
                result_msg.ERROR_MSG = f"failed to update model config. max retry exceeded"
                result_msg.CODE = RequestResult.FAIL
                return result_msg
        self._update_config_state[model_key].is_update_config_set = True
        with RollbackContext(task=self.rollback_load_model, model_key=model_key):
            if req_body.LATEST is None:
                if req_body.MDL_KEY in self._loaded_models:
                    req_body.LATEST = self._loaded_models[model_key].latest
                else:
                    raise ValueError(f"latest version of model is not defined. "
                                     f"latest version must be defined for first deploy")
            versions_be_loaded = []
            update_latest = False
            if model_key in self._loaded_models:
                for version in req_body.VERSIONS:
                    if version not in self._loaded_models[model_key].states.keys():
                        versions_be_loaded.append(version)
                if req_body.LATEST != self._loaded_models[model_key].latest:
                    update_latest = True
                if not versions_be_loaded and not update_latest:
                    return result_msg
            else:
                versions_be_loaded = req_body.VERSIONS
            specific_versions = []
            if req_body.MDL_KEY in self._loaded_models:
                with self._lock_loaded_models:
                    specific_versions += list(self._loaded_models[model_key].states.keys())
            specific_versions += versions_be_loaded
            specific_versions = [req_body.LATEST if x == -1 else x for x in specific_versions]
            s3_path = get_s3_path(model_key=model_key)
            version_policy = create_version_policy(specific_versions)
            try:
                cur_config = self.rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=s3_path)
            except Exception as exc:
                self.logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = f"failed to read config from server"
                return result_msg
            self._update_config_state[model_key].config_backup = cur_config
            new_config = model_config_to_dict(cur_config)
            new_config["versionPolicy"] = version_policy
            new_config = dict_to_model_config(new_config)

            try:
                self.rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config.encode(), target_path=s3_path)
            except Exception as exc:
                self.logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = f"failed to upload config to server"
                return result_msg
            self._update_config_state[model_key].is_uploaded = True

            for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
                url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    result_msg.CODE = RequestResult.FAIL
                    result_msg.ERROR_MSG = msg
                    return result_msg
                else:
                    self._update_config_state[model_key].is_loaded = True

            with self._lock_loaded_models:
                for version in req_body.VERSIONS:
                    if model_key in self._loaded_models:
                        if version in self._loaded_models[model_key].states:
                            self._loaded_models[model_key].states[version] += 1
                        else:
                            self._loaded_models[model_key].states = {version: 1}
                    else:
                        self._loaded_models[model_key] = ModelVersionState(states={version: 1}, latest=req_body.LATEST)

                self._loaded_models[model_key].latest = req_body.LATEST
            self._update_config_state[model_key].is_done = True
            return result_msg

    def unload_model(self, req_body: req_vo.UnloadModel = Depends(validator_unload_model)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        model_key = req_body.MDL_KEY
        if self.update_config_xin(model_key):
            for _ in range(MAX_RETRY):
                time.sleep(RETRY_DELAY)
                if not self.update_config_xin(model_key):
                    break
            else:
                raise RuntimeError(f"failed to update model config. max retry exceeded")
        self._update_config_state[model_key].is_update_config_set = True
        with RollbackContext(task=self.rollback_load_model, model_key=model_key):
            loaded_version_state = self._loaded_models[model_key].states.copy()
            for version in req_body.VERSIONS:
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
                    self._update_config_state[model_key].is_done = True
                    return result_msg

            if not set(new_specific_versions):
                for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
                    url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/unload"
                    code, msg = request_util.post(url=url)
                    if code != 0:
                        result_msg.CODE = RequestResult.FAIL
                        result_msg.ERROR_MSG = msg
                        return result_msg
                    else:
                        self._update_config_state[model_key].is_loaded = True
                with self._lock_loaded_models:
                    del self._loaded_models[model_key]
                self._update_config_state[model_key].is_done = True
                return result_msg

            version_policy = create_version_policy(list(set(new_specific_versions)))
            s3_path = get_s3_path(model_key=model_key)
            try:
                cur_config = self.rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=s3_path)
            except Exception as exc:
                self.logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = f"failed to read config from server"
                return result_msg
            self._update_config_state[model_key].config_backup = cur_config
            new_config = model_config_to_dict(cur_config)
            new_config["versionPolicy"] = version_policy
            new_config = dict_to_model_config(new_config)
            try:
                self.rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config, target_path=s3_path)
            except Exception as exc:
                self.logger.error(f"{exc.__str__()}, {traceback.format_exc()}")
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = f"failed to upload config to server"
                return result_msg
            self._update_config_state[model_key].is_uploaded = True

            for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
                url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MDL_KEY}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    result_msg.CODE = RequestResult.FAIL
                    result_msg.ERROR_MSG = msg
                    return result_msg
                else:
                    self._update_config_state[model_key].is_loaded = True
            with self._lock_loaded_models:
                self._loaded_models[model_key].states = loaded_version_state
            self._update_config_state[model_key].is_done = True
            return result_msg

    def update_latest(self, model_key: str, latest_version: int):
        if self.update_config_xin(model_key):
            for _ in range(MAX_RETRY):
                time.sleep(RETRY_DELAY)
                if not self.update_config_xin(model_key):
                    break
            else:
                raise RuntimeError(f"failed to update model config. max retry exceeded")
        with RollbackContext(task=self.remove_update_config_state, model_key=model_key):
            versions = [latest_version if x == -1 else x for x in self._loaded_models[model_key].states.keys()]
            versions = list(set(versions))
            version_policy = create_version_policy(versions=versions)
            path = f"{model_key}/{MODEL_CONFIG_FILENAME}"
            cur_config = self.rw_util.read_object(bucket_name=ModelStore.BASE_PATH, path=path)
            new_config = model_config_to_dict(cur_config)
            new_config["versionPolicy"] = version_policy
            new_config = dict_to_model_config(new_config)
            self.rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=new_config.encode(), target_path=path)
            with self._lock_loaded_models:
                self._loaded_models[model_key].latest = latest_version

    def update_config_xin(self, model_key):
        with self._lock_update_config:
            if model_key in self._update_config_state:
                return True
            else:
                self._update_config_state[model_key] = LoadModelState()
                return False

    def remove_update_config_state(self, model_key: str):
        with self._lock_update_config:
            if model_key in self._update_config_state:
                del self._update_config_state[model_key]

    def rollback_load_model(self, model_key: str):
        if model_key not in self._update_config_state:
            self.logger.warning(f"failed to find {model_key} in update_config_state")
            return
        load_model_state = self._update_config_state[model_key]
        if load_model_state.is_done:
            self.remove_update_config_state(model_key=model_key)
            return
        if load_model_state.is_uploaded:
            self.rw_util.upload_object(bucket_name=ModelStore.BASE_PATH, data=load_model_state.config_backup,
                                       target_path=f"{model_key}/{MODEL_CONFIG_FILENAME}")
        if load_model_state.is_loaded:
            for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
                url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{model_key}/load"
                code, msg = request_util.post(url=url)
                if code != 0:
                    self.logger.error(msg=msg)
        if load_model_state.is_update_config_set:
            self.remove_update_config_state(model_key=model_key)

    def get_suitable_server(self) -> str:
        return SYSTEM_ENV.TRITON_SERVER_URLS[0]
