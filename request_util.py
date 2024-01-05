from typing import Any
import requests


def post(url: str, data: dict | None = None, options: dict | None = None) -> tuple[int, Any]:
    try:
        if data is not None:
            response = requests.post(url=url, json=data)
        else:
            response = requests.post(url=url)
    except requests.exceptions.ConnectionError:
        msg = f"connection error: failed to connect {url}(triton server)"
        return -1, msg
    else:
        if response.status_code == 200:
            return 0, None
        else:
            return -1, response.json()

