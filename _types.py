from dataclasses import dataclass
from typing import Any, Optional

from pydantic import BaseModel


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
    schema_: list[tuple[str, str]]
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
    propagated: list[str] = []
    state_backup: ModelVersionState | None = None
    config_backup: str | None = None


class PropLoadModelState(BaseModel):
    is_loaded: bool = False
    is_done: bool = False
    state_backup: ModelVersionState | None = None


class PropUnloadModelState(BaseModel):
    is_done: bool = False
    is_loaded: bool = False
    is_unloaded: bool = False
    state_backup: ModelVersionState | None = None


class LoadEnsembleState(BaseModel):
    is_done: bool = False
    is_loaded: bool = False
    is_unloaded: bool = False
    is_set: bool = False
    is_del: bool = False
    propagated: list[str] = []


class PropLoadEnsembleState(BaseModel):
    is_done: bool = False
    is_loaded: bool = False
    is_unloaded: bool = False
    is_set: bool = False
    is_del: bool = False


class CreateRoutingState(BaseModel):
    is_done: bool = False
    infer_path: str | None = None
    desc_path: str | None = None
    propagated: list[str] = []


class UpdateRoutingState(BaseModel):
    is_done: bool = False
    is_route_infer_changed: bool = False
    infer_key: str | None = None
    infer_val_backup: InferenceServiceDef | None = None
    desc_key: str | None = None
    is_route_desc_changed: bool = False
    desc_val_backup: ServiceDescription | None = None
    propagated: list[str] = []


class DeleteRoutingState(BaseModel):
    is_done: bool = False
    is_route_infer_deleted: bool = False
    infer_key: str | None = None
    infer_val_backup: InferenceServiceDef | None = None
    desc_key: str | None = None
    is_route_desc_deleted: bool = False
    desc_val_backup: ServiceDescription | None = None
    propagated: list[str] = []


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


class ServiceEndpointInfo(BaseModel):
    SVC_NM: str
    PRJ_ID: str
    MDL_KEY: str | None = None
    VERSION: int | None = None
    USE_SEQUENCE: bool = False
    INPUT_SCHEMA: list[dict] | None = None
    OUTPUT_SCHEMA: list[dict] | None = None


class InferenceServiceStateInfo(BaseModel):
    MODELS: dict
    PIPELINES: list[str]
    SERVICES: list[dict]


class InferenceServiceState(BaseModel):
    MODIFY_COUNT: int
    STATE: InferenceServiceStateInfo


class ClusterInfo(BaseModel):
    REGION: str
    SERVICE_NAME: str


@dataclass
class Agreement:
    AGREE: int = 0
    DISAGREE: int = 1
    REJECT: int = 2


class TaskResult(BaseModel):
    CODE: int
    MESSAGE: str = ''
    RESULT_VALUE: Optional[Any] = None


@dataclass
class TaskResultCode:
    DONE: int = 0
    FAIL: int = 1


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