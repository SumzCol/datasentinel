from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from dataguard.core import DataGuardError
from dataguard.validation.node.result import ValidationNodeResult


T = TypeVar('T')


class RendererError(DataGuardError):
    pass


class AbstractRenderer(ABC, Generic[T]):
    @abstractmethod
    def render(self, result: ValidationNodeResult) -> T:
        pass