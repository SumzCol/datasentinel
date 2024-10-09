from dataclasses import dataclass
from datetime import datetime
from typing import List, Any

from dataguard.validation.bad_records.core import AbstractBadRecordsDataset
from dataguard.validation.status import Status


@dataclass
class RuleMetric:
    rule: str
    column: List[str]
    value: Any
    total_rows: int
    violations: int
    pass_rate: float
    pass_threshold: float
    start_time: datetime
    end_time: datetime
    bad_records: AbstractBadRecordsDataset | None
    status: Status
