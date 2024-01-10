import logging
import time
import traceback
from multiprocessing import Lock

from fastapi import APIRouter, Depends, Request, HTTPException
import pydantic
from cryptography.fernet import Fernet

import request_util
import request_vo as req_vo
import response_vo as res_vo
from rw_util import RWUtil
from utils import RollbackContext, create_version_policy, dict_to_model_config, model_config_to_dict, get_s3_path
from _types import RequestResult, ModelVersionState, LoadModelState
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
    def __init__(self, endpoints: dict | None = None):
        if endpoints is not None:
            self._endpoints: dict[str, dict[str, str]] = endpoints
        else:
            self._endpoints = {}
        self.rw_util = RWUtil()

        self._lock_loaded_models: Lock = Lock()
        self._loaded_models: dict[str, ModelVersionState] = {}

        self._lock_update_config: Lock = Lock()
        self._update_config_state: dict[str, LoadModelState] = {}

        self.router = APIRouter()
        self.logger = logging.getLogger("root")
        self.router.add_api_route("/status/loaded_models", self.get_loaded_models, methods=["GET"], response_model=res_vo.LoadedModels)
        self.router.add_api_route("/model/load", self.load_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/model/unload", self.unload_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/ensemble/load", self.load_ensemble, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/ensemble/unload", self.unload_ensemble, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/endpoint/create", self.create_endpoint, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/endpoint/remove", self.remove_endpoint, methods=["POST"], response_model=res_vo.Base)

    def get_loaded_models(self):
        result_msg = res_vo.LoadedModels(CODE=RequestResult.SUCCESS, ERROR_MSG='', LOADED_MODELS={})
        loaded_models = self._loaded_models
        result_loaded_models = {}
        for model_key, version_state in loaded_models.items():
            result_loaded_models[model_key] = {"VERSIONS": [{"VERSION": version, "REF": ref} for version, ref in version_state.states.items()], "LATEST": version_state.latest}
        result_msg.LOADED_MODELS = result_loaded_models
        return result_msg

    def add_api_route(self, path: str, func: callable, methods: list[str], response_model: pydantic.BaseModel):
        self.router.add_api_route(path, func, methods=methods, response_model=response_model)

    def create_endpoint(self, req_body: req_vo.CreateEndpoint):
        # authorize method
        # set service discover
        # give token for auth method
        # auth with depends
        # https://172.20.30.157:xxxx/project_id/service_name/infer ["POST"]
        # https://172.20.30.157:xxxx/project_id/service_name ["GET"]
        # service name must unique
        pass

    def remove_endpoint(self):
        pass

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


    # def inference(self, request: req_vo.ABC = Depends(...)):
    #     # get key from path
    #     # check monitoring state
    #     # check model name, version
    #     # request to http://triton_1:xxxx/v2/models/model_name/versions/n/infer -d DATA
    #     # save input / output if monitored
    #     return {}
