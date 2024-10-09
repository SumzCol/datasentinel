from abc import ABC, abstractmethod
from typing import Any

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult


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
