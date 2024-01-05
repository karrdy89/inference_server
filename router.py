import logging

from fastapi import APIRouter, Depends, Request, HTTPException
import pydantic
from cryptography.fernet import Fernet

import request_util
import request_vo as req_vo
import response_vo as res_vo
from _types import RequestResult
from _constants import KEY, ORIGIN, SYSTEM_ENV, RequestPath


async def validator_load_models(request: Request) -> req_vo.ListModels:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.ListModels(**body)
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
        print(decoded)
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
        self.router = APIRouter()
        self.logger = logging.getLogger("root")
        self.router.add_api_route("/model/load", self.load_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/model/unload", self.unload_model, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/service/run", self.add_endpoint, methods=["POST"], response_model=res_vo.Base)
        self.router.add_api_route("/service/stop", self.remove_endpoint, methods=["POST"], response_model=res_vo.Base)

    def add_api_route(self, path: str, func: callable, methods: list[str], response_model: pydantic.BaseModel):
        self.router.add_api_route(path, func, methods=methods, response_model=response_model)

    def add_endpoint(self, project: str, model: str, service_name: str, version: str | None = None):
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

    def load_model(self, req_body: req_vo.LoadModel = Depends(validator_load_models)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        loaded_servers = []
        for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
            url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MODEL}/load"
            code, msg = request_util.post(url=url)
            if code != 0:
                for loaded_server in loaded_servers:
                    rollback_url = loaded_server + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MODEL}/unload"
                    code, msg = request_util.post(url=rollback_url)
                    if code != 0:
                        self.logger.error(msg=msg)
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = msg
                return result_msg
            else:
                loaded_servers.append(triton_url)
        return result_msg

    def unload_model(self, req_body: req_vo.LoadModel = Depends(validator_load_models)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        unloaded_servers = []
        for triton_url in SYSTEM_ENV.TRITON_SERVER_URLS:
            url = triton_url + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MODEL}/unload"
            code, msg = request_util.post(url=url)
            if code != 0:
                for unloaded_server in unloaded_servers:
                    rollback_url = unloaded_server + f"{RequestPath.MODEL_REPOSITORY_API}/{req_body.MODEL}/load"
                    code, msg = request_util.post(url=rollback_url)
                    if code != 0:
                        self.logger.error(msg=msg)
                result_msg.CODE = RequestResult.FAIL
                result_msg.ERROR_MSG = msg
                return result_msg
            else:
                unloaded_servers.append(triton_url)
        return result_msg

    # def inference(self, request: req_vo.ABC = Depends(...)):
    #     # get key from path
    #     # check monitoring state
    #     # check model name, version
    #     # request to http://triton_1:xxxx/v2/models/model_name/versions/n/infer -d DATA
    #     # save input / output if monitored
    #     return {}
