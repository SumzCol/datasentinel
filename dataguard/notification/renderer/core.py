from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from dataguard.core import DataGuardError
from dataguard.validation.result import DataValidationResult

T = TypeVar("T")


class RendererError(DataGuardError):
    pass


class AbstractRenderer(ABC, Generic[T]):
    @abstractmethod
    def render(self, result: DataValidationResult) -> T:
        pass
