from pydantic import BaseModel, Extra


class RegisterService(BaseModel):
    URL: str
    LABEL: str
    TAG: str | None = None
    TOKEN: bytes | None = None
    REGION: str
    ID: str

    class Config:
        extra = Extra.forbid


class ListModels(BaseModel):
    PRJ_ID: str
    MODELS: list[str]
