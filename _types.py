from dataclasses import dataclass


@dataclass
class RequestResult:
    SUCCESS: str = "00"
    FAIL: str = "10"
