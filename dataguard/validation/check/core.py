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
        """Return the name of the check."""
        return self._name

    @property
    def level(self) -> CheckLevel:
        """Return the level of the check."""
        return self._level

    @abstractmethod
    def evaluate(self, df: Any) -> CheckResult:
        """Evaluate the given dataframe against the check rules."""
