from dataclasses import dataclass
from datetime import datetime
from typing import List, Any

from cuallee import CheckLevel

from dataguard.validation.result.core import Status, AbstractBadRecordsDataset


@dataclass
class RuleMetric:
    column: List[str]
    rule: str
    value: Any
    total_rows: int
    violations: int
    pass_rate: float
    pass_threshold: float
    status: Status
    bad_records: AbstractBadRecordsDataset | None
