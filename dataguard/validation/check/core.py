from abc import ABC, abstractmethod
from typing import Any

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult


class CheckError(Exception):
    pass


class UnsupportedDataframeTypeError(CheckError):
    pass


class EmptyCheckError(CheckError):
    pass


class AbstractCheck(ABC):
    """Base class for all data quality check implementations."""
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
    def validate(self, df: Any) -> CheckResult:
        """Validate the given dataframe.

        Args:
            df: The dataframe to validate.

        Returns:
            The result of data quality check applied to the dataframe.
        """
