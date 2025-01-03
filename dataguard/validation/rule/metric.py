from dataclasses import dataclass
from typing import Dict, Any

from dataguard.validation.bad_records.core import AbstractBadRecordsDataset
from dataguard.validation.status import Status


@dataclass
class RuleMetric:
    id: int
    rule: str
    column: str
    value: str
    rows: int
    violations: int
    pass_rate: float
    pass_threshold: float
    options: Dict[str, Any] | None
    bad_records: AbstractBadRecordsDataset | None

    @property
    def status(self):
        return Status.PASS if self.pass_rate >= self.pass_threshold else Status.FAIL

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "rule": self.rule,
            "column": self.column,
            "value": self.value,
            "rows": self.rows,
            "violations": self.violations,
            "pass_rate": self.pass_rate,
            "pass_threshold": self.pass_threshold,
            "options": self.options,
            "bad_records": self.bad_records,
            "status": self.status.value
        }