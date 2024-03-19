import traceback

from fastapi import HTTPException, Request
from cryptography.fernet import Fernet

import request_vo as req_vo
from _constants import ORIGIN, KEY, ROOT_LOGGER


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


async def validator_update_model_latest(request: Request) -> req_vo.ModelLatest:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.ModelLatest(**body)
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


async def validator_update_endpoint(request: Request) -> req_vo.UpdateEndpoint:
    validate_header(request.headers)
    try:
        body = await request.json()
        response = req_vo.UpdateEndpoint(**body)
    except Exception:
        raise HTTPException(status_code=422, detail="Validation Error")
    else:
        return response


async def validate_token(request: Request) -> None:
    validate_header(request.headers)


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
    except Exception as exc:
        ROOT_LOGGER.error(f"{exc.__str__()}, {traceback.format_exc()}")
        return False
    else:
        if ORIGIN == decoded:
            return True
        else:
            return False
