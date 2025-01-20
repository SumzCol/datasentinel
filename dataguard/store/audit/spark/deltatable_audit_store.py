from dataguard.store.audit.core import AbstractAuditStore
from dataguard.store.audit.row import AuditRow


class DeltaTableAuditStore(AbstractAuditStore):
    def __init__(self, name: str, disabled: bool):
        super().__init__(name, disabled)

    def append(self, row: AuditRow):
        raise NotImplementedError