from dataclasses import dataclass
from datetime import datetime
from typing import Union, List, Any

from cuallee import CheckLevel

from dataguard.validation.result.core import Status, AbstractBadRecordsDataset


@dataclass
class RuleMetric:
    id: int
    timestamp: datetime
    check: str
    level: CheckLevel
    column: List[str]
    rule: str
    value: Any
    total_rows: int
    failed_rows: int
    pass_rate: float
    pass_threshold: float
    status: Status
    bad_records: AbstractBadRecordsDataset | None
