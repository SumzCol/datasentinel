from dataclasses import dataclass
from datetime import datetime
from typing import List

from dataguard.validation.check.core import CheckLevel
from dataguard.validation.result.rule_metric import RuleMetric
from dataguard.validation.result.core import Status


@dataclass
class CheckResult:
    name: str
    level: CheckLevel
    start_time: datetime
    end_time: datetime
    status: Status
    rule_metrics: List[RuleMetric]