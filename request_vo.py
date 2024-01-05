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
    MODEL: str
