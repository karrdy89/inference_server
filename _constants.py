import os
import configparser
from dataclasses import dataclass
from cryptography.fernet import Fernet
import uuid

from pydantic import BaseModel


ROOT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

KEY = Fernet.generate_key()
ORIGIN = str(uuid.uuid4())
TOKEN = Fernet(KEY).encrypt(ORIGIN.encode()).decode()


class SystemEnvironments(BaseModel):
    NAME: str = "INFERENCE_SERVER"
    API_SERVER: str
    TRITON_SERVER_URLS: list[str]


configs = configparser.ConfigParser(allow_no_value=True)
configs.read(ROOT_DIR+"/config/server_config.ini")

SYSTEM_ENV = SystemEnvironments(API_SERVER=configs["DEFAULT"]["API_SERVER"],
                                TRITON_SERVER_URLS=(configs["TRITON_SERVER"]["URLS"].split(',')))


@dataclass
class RequestPath:
    REGISTER_SERVICE: str = "/api/v0/service/register"
    CHECK_SERVICE_CONNECTION: str = "/api/v0/service"
    MODEL_REPOSITORY_API: str = "/v2/repository/models"
    MODEL_API: str = "/v2/models"
