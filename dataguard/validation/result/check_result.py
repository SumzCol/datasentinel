from dataclasses import dataclass
from typing import List

from dataguard.validation.check.core import CheckLevel
from dataguard.validation.result import Status, RuleMetric


@dataclass
class CheckResult:
    name: str
    level: CheckLevel
    status: Status
    rule_metrics: List[RuleMetric]