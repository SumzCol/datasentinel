from abc import ABC, abstractmethod
from typing import Any

from dataguard.validation.suite.result import ValidationSuiteResult


class AbstractRenderer(ABC):

    @abstractmethod
    def render(self, validation_suite_result: ValidationSuiteResult) -> Any:
        pass