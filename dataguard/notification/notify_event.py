from enum import Enum


class NotifyOnEvent(Enum):
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    ALL = "ALL"
