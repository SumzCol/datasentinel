from abc import ABC, abstractmethod

from dataguard.validation.suite.result import ValidationSuiteResult


class AbstractNotifier(ABC):

    def __init__(self, name: str, disabled: bool = False):
        self._name = name
        self._disabled = disabled

    @property
    def name(self) -> str:
        return self._name

    @property
    def disabled(self) -> bool:
        return self._disabled

    @abstractmethod
    def notify(self, validation_suite_result: ValidationSuiteResult):
        pass