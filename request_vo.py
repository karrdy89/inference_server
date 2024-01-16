from pydantic import BaseModel, Extra

from _types import InferenceIO


class RegisterService(BaseModel):
    URL: str
    LABEL: str
    TAG: str | None = None
    TOKEN: str | None = None
    REGION: str
    ID: str

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
