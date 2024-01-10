import configparser
from typing import Any

import boto3
import botocore
from boto3_type_annotations.s3 import ServiceResource
from min_db import Connector

from _constants import ROOT_DIR


class RWUtil:
    def __init__(self):
        config_parser = configparser.ConfigParser(allow_no_value=True)
        config_parser.read(ROOT_DIR + "/config/server_config.ini")
        self._s3: ServiceResource = boto3.resource(service_name="s3",
                                                   endpoint_url=config_parser["S3"]["ENDPOINT"],
                                                   aws_access_key_id=config_parser["S3"]["ACCESS_KEY"],
                                                   aws_secret_access_key=config_parser["S3"]["SECRET_KEY"],
                                                   use_ssl=True,
                                                   verify=bool(int(config_parser["S3"]["USE_SECURE"]))
                                                   )
        self._connector: Connector = Connector(db_type=config_parser["MANAGE_DB"]["TYPE"],
                                               ip=config_parser["MANAGE_DB"]["IP"],
                                               port=int(config_parser["MANAGE_DB"]["PORT"]),
                                               user=config_parser["MANAGE_DB"]["USER"],
                                               password=config_parser["MANAGE_DB"]["PASSWORD"],
                                               db=config_parser["MANAGE_DB"]["DB"],
                                               query_path=ROOT_DIR + "/queries/base.xml",
                                               session_pool_max=5)

    def upload_object(self, bucket_name: str, data: Any, target_path: str):
        object_ = self._s3.Object(bucket_name=bucket_name, key=target_path)
        object_.put(Body=data)

    def read_object(self, bucket_name: str, path: str) -> str | None:
        obj = self._s3.Object(bucket_name=bucket_name, key=path)
        try:
            object_ = obj.get()['Body'].read().decode('utf-8')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return None
        else:
            return object_
