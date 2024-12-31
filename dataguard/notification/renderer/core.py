from abc import ABC, abstractmethod
from typing import Any

from dataguard.validation.node.result import ValidationNodeResult


class AbstractRenderer(ABC):
    @abstractmethod
    def render(self, result: ValidationNodeResult) -> Any:
        pass