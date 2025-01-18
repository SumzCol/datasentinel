from datetime import datetime
from typing import List, Dict, Any

from pydantic.dataclasses import dataclass

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@dataclass(frozen=True)
class CheckResult:
    """Represent the result of a check."""
    name: str
    level: CheckLevel
    check_class: str
    start_time: datetime
    end_time: datetime
    rule_metrics: List[RuleMetric]

    @property
    def status(self):
        """Return the status of the check."""
        return (
            Status.PASS
            if all([metric.status == Status.PASS for metric in self.rule_metrics])
            else Status.FAIL
        )

    @property
    def failed_rules(self) -> List[RuleMetric]:
        """Return the failed rules."""
        return [metric for metric in self.rule_metrics if metric.status == Status.FAIL]

    @property
    def failed_rules_count(self) -> int:
        return len(self.failed_rules)

    def to_dict(self) -> Dict[str, Any]:
        """Return the result as a dictionary."""
        return {
            "name": self.name,
            "level": self.level.name,
            "check_class": self.check_class,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "rule_metrics":  [
                rule_metric.to_dict()
                for rule_metric in self.rule_metrics
            ],
            "status": self.status.value
        }
