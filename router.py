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
        self.router.add_api_route("/model/reload", self.reload_model, methods=["POST"], response_model=res_vo.Base)
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

    def reload_model(self, req_body: req_vo.ListModels = Depends(validator_load_models)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        for model in req_body.MODELS:
            for triton_url in SYSTEM_ENV.TRITON_SERVERS:
                url = f"{triton_url}/{RequestPath.MODEL_REPOSITORY_API}/{model}/load"
                code, msg = request_util.post(url=url)
        # request to all available triton server
        # return result
        return result_msg

    def unload_model(self, req_body: req_vo.ListModels = Depends(validator_load_models)):
        result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
        return result_msg

    # def inference(self, request: req_vo.ABC = Depends(...)):
    #     # get key from path
    #     # check monitoring state
    #     # check model name, version
    #     # request to http://triton_1:xxxx/v2/models/model_name/versions/n/infer -d DATA
    #     # save input / output if monitored
    #     return {}
