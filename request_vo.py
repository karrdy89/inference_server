from pydantic import BaseModel, Extra


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
    MLD_KEY: str
    VERSION: int | None = None
    SVC_NM: str
