from typing import Callable, Literal

from google.protobuf import text_format
from google.protobuf.json_format import ParseDict, MessageToDict
from protobuf import model_config_pb2
from fastapi.responses import JSONResponse
import numpy as np

from _constants import MODEL_CONFIG_FILENAME
from _types import InterfaceSchema, ServiceDescription, InferenceIO
from request_vo import InputSpec


class RollbackContext:
    def __init__(self, task: Callable, **kwargs):
        self._task: Callable = task
        self._args: dict = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._task(**self._args)


def model_config_to_dict(config: str) -> dict:
    pb_message = text_format.Parse(config, model_config_pb2.ModelConfig())
    return MessageToDict(pb_message)


def dict_to_model_config(config: dict) -> str:
    config = ParseDict(config, model_config_pb2.ModelConfig())
    return text_format.MessageToString(config)


def create_version_policy(versions: list) -> dict:
    return {"specific": {"versions": versions}}


def get_s3_path(model_key: str) -> str:
    return f"{model_key}/{MODEL_CONFIG_FILENAME}"


def make_inference_input(schema: list[tuple[str, str]], inputs: list[InputSpec]) -> JSONResponse | dict:
    result = {"inputs": []}
    req_inputs = {}
    for input_spec in inputs:
        req_inputs[input_spec.name] = input_spec.data
    for input_name, data_type in schema:
        try:
            data = req_inputs[input_name]
        except KeyError:
            return JSONResponse(status_code=400, content={"error": f"input field is missing '{input_name}'"})
        else:
            shape = list(np.shape(data))
            result["inputs"].append({"name": input_name, "shape": shape, "datatype": data_type,
                                     "data": data})
    return result


def create_service_path(project: str, name: str) -> tuple[str, str]:
    return f"/{project}/{name}/infer", f"/{project}/{name}/desc"


def create_kserve_inference_path(model_name: str, version: int | None = None) -> str:
    if version is None:
        return f"/v2/models/{model_name}/infer"
    else:
        return f"/v2/models/{model_name}/versions/{version}/infer"


def create_service_description(url: str, input_schema: list[InferenceIO], output_schema: list[InferenceIO]) -> ServiceDescription:
    inputs = []
    for field in input_schema:
        inputs.append({"name": field.name, "data": f"data of {field.name} # datatype: {field.datatype}, shape: {field.dims}"})
    outputs = []
    for field in output_schema:
        outputs.append({"name": field.name, "datatype": field.datatype, "shape": field.dims, "data": f"inference result"})
    input_schema = InterfaceSchema(DATA={"inputs": inputs})
    output_schema = InterfaceSchema(DATA={"outputs": outputs})
    return ServiceDescription(URL=url, REQUEST=input_schema, RESPONSE=output_schema)


def calculate_cpu_usage(stats: dict):
    UsageDelta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
    SystemDelta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
    cpuPercent = (UsageDelta / SystemDelta) * (stats["cpu_stats"]["online_cpus"]) * 100
    percent = round(cpuPercent, 2)
    return percent


def calculate_network_bytes(d):
    networks = graceful_chain_get(d, "networks")
    if not networks:
        return 0, 0
    r = 0
    t = 0
    for if_name, data in networks.items():
        r += data["rx_bytes"]
        t += data["tx_bytes"]
    return r, t


def calculate_block_bytes(d):
    block_stats = graceful_chain_get(d, "blkio_stats")
    if not block_stats:
        return 0, 0
    r = 0
    w = 0
    if block_stats["io_service_bytes_recursive"] is not None:
        for _ in block_stats["io_service_bytes_recursive"]:
            if _["op"] == "read":
                r += _["value"]
            elif _["op"] == "write":
                w += _["value"]
    return r, w


def graceful_chain_get(d, *args, default=None):
    t = d
    for a in args:
        try:
            t = t[a]
        except (KeyError, ValueError, TypeError, AttributeError):
            return default
    return t


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"
