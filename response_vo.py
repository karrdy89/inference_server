from pydantic import BaseModel

from _types import InferenceServiceState


class Base(BaseModel):
    CODE: str
    ERROR_MSG: str


class Agreement(Base):
    AGREEMENT: int  # 0: ok, 1: not ok, 2: oppose


class RspInferenceServiceState(Base):
    SERVICE_STATE: InferenceServiceState | None = None


class LoadedModels(Base):
    LOADED_MODELS: dict


class ServiceDescribe(Base):
    SERVICE_DESC: dict


class ListEndpoint(Base):
    ENDPOINTS: list



