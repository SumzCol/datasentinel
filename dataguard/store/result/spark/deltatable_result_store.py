from dataguard.store.result.core import AbstractResultStore
from dataguard.validation.node.result import ValidationNodeResult


class DeltaTableResultStore(AbstractResultStore):
    def __init__(self, name: str, disabled: bool):
        super().__init__(name, disabled)

    def store(self, result: ValidationNodeResult):
        pass