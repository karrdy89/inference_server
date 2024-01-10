from pydantic import BaseModel

from dataclasses import dataclass


@dataclass
class RequestResult:
    SUCCESS: str = "00"
    FAIL: str = "10"


class ModelVersionState(BaseModel):
    states: dict[int, int]
    latest: int


class LoadModelState(BaseModel):
    is_done: bool = False
    is_uploaded: bool = False
    is_loaded: bool = False
    is_update_config_set: bool = False
    config_backup: str | None = None
