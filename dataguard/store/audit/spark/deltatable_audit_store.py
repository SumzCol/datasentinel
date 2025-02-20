from dataguard.store.audit.core import AbstractAuditStore
from dataguard.store.audit.row import BaseAuditRow


class DeltaTableAuditStore(AbstractAuditStore):
    def __init__(self, name: str, disabled: bool):
        super().__init__(name, disabled)

    def append(self, row: BaseAuditRow):
        raise NotImplementedError
