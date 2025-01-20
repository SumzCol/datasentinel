from typing import Dict, Any, Optional

from pydantic import ConfigDict
from pydantic.dataclasses import dataclass

from dataguard.validation.failed_rows_dataset.core import AbstractFailedRowsDataset
from dataguard.validation.status import Status


@dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
class RuleMetric:
    """Represent the result metric of a data quality rule.

    Attributes:
        id: The id of the rule metric.
        rule: The name of the rule that was evaluated.
        column: The column or columns that the rule evaluated.
        value: The value of the rule.
        rows: The number of rows analyzed.
        violations: The number of rows that didn't pass the rule.
        pass_rate: The pass rate representing the percentage of rows that passed the rule.
        pass_threshold: The pass threshold set for the rule.
        options: The options set for the rule if any.
        failed_rows_dataset: The failed rows dataset containing the rows that failed the rule
            if there were any violations, and if they were computed.
    """
    id: int
    rule: str
    column: str
    value: str
    rows: int
    violations: int
    pass_rate: float
    pass_threshold: float
    options: Optional[Dict[str, Any]] = None
    failed_rows_dataset: Optional[AbstractFailedRowsDataset] = None

    @property
    def status(self):
        """Return the status of the rule."""
        return Status.PASS if self.pass_rate >= self.pass_threshold else Status.FAIL

    def to_dict(self) -> Dict[str, Any]:
        """Return the rule metric as a dictionary."""
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
            "failed_rows_dataset": self.failed_rows_dataset,
            "status": self.status.value
        }