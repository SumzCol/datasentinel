from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from dataguard.core import DataGuardError
from dataguard.validation.result import DataValidationResult


T = TypeVar("T")


class RendererError(DataGuardError):
    pass


class AbstractRenderer(ABC, Generic[T]):
    """Base class for all renderer implementations."""

    @abstractmethod
    def render(self, result: DataValidationResult) -> T:
        """Render a data validation result into a notification message."""
