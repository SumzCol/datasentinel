from enum import Enum


class Status(Enum):
    """Enum for result statuses"""

    FAIL = "FAIL"
    PASS = "PASS"  # noqa: S105
    NO_RUN = "NO_RUN"
