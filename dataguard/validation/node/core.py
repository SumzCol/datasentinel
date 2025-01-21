from enum import Enum


class ValidationNodeError(Exception):
    pass


class NotifyOnEvent(Enum):
    """Enum for notification trigger events on a validation node.

    Attributes:
        FAIL: Send notification when a validation node fails.
        PASS: Send notification when a validation node passes.
        ALL: Always send notification.
    """
    FAIL = "FAIL"
    PASS = "PASS"
    ALL = "ALL"
