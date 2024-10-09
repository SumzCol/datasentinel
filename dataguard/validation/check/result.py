from dataclasses import dataclass
from datetime import datetime
from typing import List

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@dataclass
class CheckResult:
    name: str
    level: CheckLevel
    start_time: datetime
    end_time: datetime
    rule_metrics: List[RuleMetric]
    status: Status

    def failed_rules(self) -> List[RuleMetric]:
        return [metric for metric in self.rule_metrics if metric.status == Status.FAIL]

    def count_failed_rules(self) -> int:
        return len(self.failed_rules())
