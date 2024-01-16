from typing import Any
import requests

from _constants import SYSTEM_ENV


def post(url: str, data: dict | None = None, options: dict | None = None) -> tuple[int, Any]:
    try:
        if data is not None:
            response = requests.post(url=url, json=data)
        else:
            response = requests.post(url=url)
    except requests.exceptions.ConnectionError:
        msg = f"connection error: failed to connect triton server"
        return -1, msg
    else:
        if response.status_code == 200:
            if response.text == '':
                return 0, None
            else:
                return 0, response.json()
        else:
            return -1, response.text


def get(url: str, options: dict | None = None) -> tuple[int, Any]:
    try:
        response = requests.get(url=url)
    except requests.exceptions.ConnectionError:
        msg = f"connection error: failed to connect triton server"
        return -1, msg
    else:
        if response.status_code == 200:
            if response.text == '':
                return 0, None
            else:
                return 0, response.json()
        else:
            return -1, response.text


def post_to_system(url: str, data: dict, options: dict | None = None, headers: dict | None = None,
                   target_system_name: str | None = None) -> tuple[int, Any]:
    args = {"url": url, "json": data}
    if headers is not None:
        args["headers"] = headers
    if not SYSTEM_ENV.VERIFY_SSL:
        args["verify"] = False
    try:
        response = requests.post(**args)
    except requests.exceptions.ConnectionError:
        msg = "connection error"
        if target_system_name is not None:
            msg = msg + f": can't make connection with {target_system_name}"
        return -1, msg
    else:
        if response.status_code == 200:
            if response.json()["CODE"] != "00":
                return -1, response.json()["ERROR_MSG"]
        else:
            msg = f"internal server error. status code: {str(response.status_code)}"
            if target_system_name is not None:
                msg = f"internal server error on {target_system_name}. status code: {str(response.status_code)}"
            return -1, msg
    return 0, response.json()


def get_from_system(url: str, options: dict | None = None, headers: dict | None = None,
                    target_system_name: str | None = None) -> tuple[int, Any]:
    try:
        response = requests.get(url=url, verify=False)
    except requests.exceptions.ConnectionError:
        msg = "connection error"
        if target_system_name is not None:
            msg = msg + f": can't make connection with {target_system_name}"
        return -1, msg
    else:
        if response.status_code == 200:
            if response.json()["CODE"] != "00":
                return -1, response.json()["ERROR_MSG"]
        else:
            msg = f"internal server error. status code: {str(response.status_code)}"
            if target_system_name is not None:
                msg = f"internal server error on {target_system_name}. status code: {str(response.status_code)}"
            return -1, msg
    return 0, response.json()
