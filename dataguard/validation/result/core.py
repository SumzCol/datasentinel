from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Any

from markdown_it.common.html_re import attribute

from dataguard.validation.result.rule_metric import RuleMetric


class Status(Enum):
    FAIL = "fail"
    PASS = "pass"


class AbstractCheckResult(ABC):

    def __init__(self, status: Status, rule_metrics: Dict[str, RuleMetric]):
        self._status = status
        self._rule_metrics = rule_metrics

    @property
    def rule_metrics(self) -> Dict[str, RuleMetric]:
        return self._rule_metrics

    @property
    def status(self) -> Status:
        return self._status

    @abstractmethod
    def get_failed_rows(self, top: int = 100) -> List[Dict[str, Any]] | None:
        pass
