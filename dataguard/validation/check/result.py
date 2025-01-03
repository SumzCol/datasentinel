from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@dataclass
class CheckResult:
    name: str
    level: CheckLevel
    type: str
    start_time: datetime
    end_time: datetime
    rule_metrics: List[RuleMetric]

    @property
    def status(self):
        return (
            Status.PASS
            if all([metric.status == Status.PASS for metric in self.rule_metrics])
            else Status.FAIL
        )

    @property
    def failed_rules(self) -> List[RuleMetric]:
        return [metric for metric in self.rule_metrics if metric.status == Status.FAIL]

    def failed_rules_count(self) -> int:
        return len(self.failed_rules)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "level": self.level.name,
            "type": self.type,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "rule_metrics":  [
                rule_metric.to_dict()
                for rule_metric in self.rule_metrics
            ],
            "status": self.status.value
        }
