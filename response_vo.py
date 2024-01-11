from pydantic import BaseModel


class Base(BaseModel):
    CODE: str
    ERROR_MSG: str


class LoadedModels(Base):
    LOADED_MODELS: dict


class ServiceDescribe(Base):
    SERVICE_DESC: dict


