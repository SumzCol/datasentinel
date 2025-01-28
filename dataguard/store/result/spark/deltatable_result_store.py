from dataguard.store.result.core import AbstractResultStore
from dataguard.validation.result import DataValidationResult


class DeltaTableResultStore(AbstractResultStore):
    def __init__(self, name: str, disabled: bool):
        super().__init__(name, disabled)

    def store(self, result: DataValidationResult):
        pass