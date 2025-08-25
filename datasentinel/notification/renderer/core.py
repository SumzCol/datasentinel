from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from datasentinel.core import DataGuardError
from datasentinel.validation.result import DataValidationResult


T = TypeVar("T")


class RendererError(DataGuardError):
    pass


class AbstractRenderer(ABC, Generic[T]):
    """Base class for all renderer implementations."""

    @abstractmethod
    def render(self, result: DataValidationResult) -> T:
        """Render a data validation result into a notification message."""
