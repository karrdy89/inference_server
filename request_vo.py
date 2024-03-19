from pydantic import BaseModel, Extra

from _types import InferenceIO, BaseServerStats


class RegisterService(BaseModel):
    URL: str
    LABEL: str
    TAG: str | None = None
    REGION: str | None = None
    TOKEN: str | None = None
    HEALTH_CHECK_PATH: str | None = None

    class Config:
        extra = Extra.forbid


class LoadModel(BaseModel):
    PRJ_ID: str
    MDL_KEY: str
    VERSIONS: list
    LATEST: int | None


class UnloadModel(BaseModel):
    PRJ_ID: str
    MDL_KEY: str
    VERSIONS: list


class LoadEnsemble(BaseModel):
    PRJ_ID: str
    MDL_KEY: str


class CreateEndpoint(BaseModel):
    PRJ_ID: str
    EP_ID: str
    MDL_KEY: str
    INPUT_SCHEMA: list[InferenceIO]
    OUTPUT_SCHEMA: list[InferenceIO]
    VERSION: int | None = None
    SVC_NM: str
    USE_SEQUENCE: bool = False


class RemoveEndpoint(BaseModel):
    PRJ_ID: str
    SVC_NM: str


class InputSpec(BaseModel):
    name: str
    data: list


class InferenceInput(BaseModel):
    inputs: list[InputSpec]


class UpdateEndpoint(BaseModel):
    PRJ_ID: str
    MDL_KEY: str
    INPUT_SCHEMA: list[InferenceIO]
    OUTPUT_SCHEMA: list[InferenceIO]
    VERSION: int | None = None
    SVC_NM: str
    USE_SEQUENCE: bool = False


class ModelLatest(BaseModel):
    PRJ_ID: str
    MDL_KEY: str
    LATEST_VER: int


class ServerStat(BaseModel):
    NAME: str
    STATS: BaseServerStats


class ServerStats(BaseModel):
    SERVER_STATS: ServerStat
    REGION: str | None = None
    INTERVAL: int
    URL: str


class PropLoadModel(BaseModel):
    DATA: dict | None
    KEY: str
    VER: int


class Key(BaseModel):
    KEY: str


class PropUnloadModel(BaseModel):
    DATA: dict | None = None
    KEY: str
    VER: int
    IS_TRITON_UPDATED: bool = False
    IS_DELETED: bool = False


class PropLoadEnsemble(BaseModel):
    KEY: str
    VER: int
    PIPES: list[str] = []


class PropCreateEndpoint(BaseModel):
    INFER_KEY: str
    INFER_VAL: dict
    DESC_KEY: str
    DESC_VAL: dict
    VER: int


class PropUpdateModelLatest(BaseModel):
    KEY: str
    STATE: dict
    VER: int


class OverwriteModelState(BaseModel):
    KEY: str
    STATE: dict


class OverwriteEndpoints(BaseModel):
    EP_INFER: dict
    EP_DESC: dict


class PropRemoveEndpoint(BaseModel):
    PRJ_ID: str
    SVC_NM: str
    VER: int


