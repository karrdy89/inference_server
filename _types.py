from pydantic import BaseModel

from dataclasses import dataclass


@dataclass
class RequestResult:
    SUCCESS: str = "00"
    FAIL: str = "10"


class InferenceIO(BaseModel):
    name: str
    datatype: str
    dims: list


@dataclass
class InferenceServiceDef:
    path: str
    schema: list[tuple[str, str]]
    url: str | None = None


@dataclass
class InterfaceSchema:
    DATA: dict[str, list]


class ServiceDescription(BaseModel):
    URL: str
    REQUEST: InterfaceSchema
    RESPONSE: InterfaceSchema


class ModelVersionState(BaseModel):
    states: dict[int, int]
    latest: int


class LoadModelState(BaseModel):
    is_done: bool = False
    is_uploaded: bool = False
    is_loaded: bool = False
    is_update_config_set: bool = False
    config_backup: str | None = None


class BaseServerStats(BaseModel):
    HEALTHY: bool = False
    DETAILS: dict = {}
    SERVERS: list = []


class BaseContainerStats(BaseModel):
    CPU_USAGE: float | None = None
    MEM_USAGE: int | None = None
    MEM_LIMIT: int | None = None
    NET_IN: int | None = None
    NET_OUT: int | None = None
    DISK_IN: int | None = None
    DISK_OUT: int | None = None


DATATYPE_MAP = {
    "TYPE_BOOL": "BOOL",
    "TYPE_UINT8": "UINT8",
    "TYPE_UINT16": "UINT16",
    "TYPE_UINT32": "UINT32",
    "TYPE_UINT64": "UINT64",
    "TYPE_INT8": "INT8",
    "TYPE_INT16": "INT16",
    "TYPE_INT32": "INT32",
    "TYPE_INT64": "INT64",
    "TYPE_FP16": "FP16",
    "TYPE_FP32": "FP32",
    "TYPE_FP64": "FP64",
    "TYPE_STRING": "BYTES",
    "TYPE_BF16": "BF16"
}