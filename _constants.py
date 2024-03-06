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

RETRY_DELAY = 0.2
MAX_RETRY = 50

MODEL_CONFIG_FILENAME = "config.pbtxt"


@dataclass
class ModelStore:
    BASE_PATH: str = "models"


class SystemEnvironments(BaseModel):
    NAME: str = "INFERENCE_SERVER"
    HC_INTERVAL: int
    CONTAINER_NAME: str
    DISCOVER_URL: str
    DISCOVER_TAG: str
    DISCOVER_REGION: str
    API_SERVER: str
    VERIFY_SSL: bool
    SSL_KEY: str
    SSL_CERT: str
    SSL_CA_CERT: str
    TRITON_SERVER_URL: str
    TRITON_SERVER_NAME: str
    TRITON_CONTAINER_NAME: str


configs = configparser.ConfigParser(allow_no_value=True)
configs.read(ROOT_DIR + "/config/server_config.ini")


SYSTEM_ENV = SystemEnvironments(API_SERVER=configs["DEFAULT"]["API_SERVER"],
                                HC_INTERVAL=int(configs["DEFAULT"]["HC_INTERVAL"]),
                                CONTAINER_NAME=configs["DEFAULT"]["CONTAINER_NAME"],
                                SSL_KEY=configs["SSL"]["KEY_FILE"],
                                SSL_CERT=configs["SSL"]["CERT_FILE"],
                                SSL_CA_CERT=configs["SSL"]["CA_CERT_FILE"],
                                DISCOVER_URL=configs["SERVICE_DISCOVER"]["URL"],
                                DISCOVER_TAG=configs["SERVICE_DISCOVER"]["TAG"],
                                DISCOVER_REGION=configs["SERVICE_DISCOVER"]["REGION"],
                                VERIFY_SSL=bool(int(configs["DEFAULT"]["VERIFY_SSL"])),
                                TRITON_SERVER_URL=configs["TRITON_SERVER"]["URL"],
                                TRITON_SERVER_NAME="TRITON",
                                TRITON_CONTAINER_NAME=configs["TRITON_SERVER"]["CONTAINER_NAME"])


@dataclass
class RequestPath:
    REGISTER_SERVICE: str = "/api/v0/service/register"
    CHECK_SERVICE_CONNECTION: str = "/api/v0/service/stats/update"
    INIT_SERVICES: str = "/api/v0/inference/state/services/ready"
    TRITON_HEALTH_CHECK_API: str = "/v2/health/ready"
    MODEL_REPOSITORY_API: str = "/v2/repository/models"
    MODEL_API: str = "/v2/models"
