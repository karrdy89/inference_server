from typing import Callable, Literal

from google.protobuf import text_format
from google.protobuf.json_format import ParseDict, MessageToDict
from protobuf import model_config_pb2

from _constants import MODEL_CONFIG_FILENAME


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
