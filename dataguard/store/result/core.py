from abc import ABC, abstractmethod

from dataguard.validation.node.result import ValidationNodeResult


class ResultStoreError(Exception):
    pass


class ResultStoreManagerError(Exception):
    pass


class ResultStoreAlreadyExistsError(ResultStoreManagerError):
    pass


class ResultStoreNotFoundError(ResultStoreManagerError):
    pass


class AbstractResultStore(ABC):
    def __init__(self, name: str, disabled: bool):
        self._name = name
        self._disabled = disabled

    @property
    def name(self):
        return self._name

    @property
    def disabled(self):
        return self._disabled

    @abstractmethod
    def store(self, result: ValidationNodeResult):
        """Store a checklist result"""