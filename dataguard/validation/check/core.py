from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from dataguard.validation.result.check_result import CheckResult


class CheckLevel(Enum):
    ERROR = 0
    WARNING = 1


class AbstractCheck(ABC):

    def __init__(
            self,
            level: CheckLevel,
            name: str,
    ):
        self._level = level
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def level(self) -> CheckLevel:
        return self._level

    @abstractmethod
    def check(self, data: Any) -> CheckResult:
        pass
