from typing import Any

from dataguard.store.audit.row import AuditRow


class AuditRowBuilder:
    def __init__(self):
        self._row_dict = {}

    def add_or_replace_field(self, name: str, value: Any):
        self._row_dict[name] = value
        return self

    def add_or_replace_fields(self, fields: dict[str, Any]):
        self._row_dict.update(fields)
        return self

    def build(self, reset: bool = False) -> AuditRow:
        if not reset:
            return AuditRow(**self._row_dict)

        audit_row = AuditRow(**self._row_dict)
        self._row_dict = {}
        return audit_row