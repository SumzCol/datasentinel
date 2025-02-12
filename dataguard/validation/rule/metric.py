from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple, Callable

from pydantic import ConfigDict, Field, model_validator, field_validator
from pydantic.dataclasses import dataclass
from typing_extensions import Self

from dataguard.validation.failed_rows_dataset.core import AbstractFailedRowsDataset
from dataguard.validation.status import Status


@dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
class RuleMetric:
    """Represent the result metric of a data quality rule.

    Attributes:
        id: The id of the rule metric.
        rule: The name of the rule that was evaluated.
        column: The column or columns that the rule evaluated.
        id_columns: The ID columns used to identify failed rows if they were specified.
        value: The value of the rule.
        rows: The number of rows analyzed.
        violations: The number of rows that didn't pass the rule.
        pass_rate: The pass rate representing the percentage of rows that passed the rule.
        pass_threshold: The pass threshold set for the rule.
        options: The options set for the rule if any.
        failed_rows_dataset: The failed rows dataset containing the rows that failed the rule
            if there were any violations and if they were computed.
    """
    id: int
    rule: str
    rows: int
    violations: int
    pass_rate: float
    pass_threshold: float
    value: Optional[int | float | str | datetime | date | List | Callable] = None
    options: Optional[Dict[str, Any]] = None
    column: Optional[List[str]] = Field(default_factory=list)
    id_columns: Optional[List[str]] = Field(default_factory=list)
    failed_rows_dataset: Optional[AbstractFailedRowsDataset] = None

    @model_validator(mode="after")
    def validate_model(self) -> Self:
        if self.violations > self.rows:
            raise ValueError("Violations cannot be greater than rows")

        return self

    @field_validator("column", mode="before")
    def validate_columns(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [value]
        if isinstance(value, tuple) or isinstance(value, set):
            return list(value)
        return value

    @property
    def status(self):
        """Return the status of the rule."""
        return Status.PASS if self.pass_rate >= self.pass_threshold else Status.FAIL

    def to_dict(self) -> Dict[str, Any]:
        """Return the rule metric as a dictionary."""
        return {
            "id": self.id,
            "rule": self.rule,
            "columns": self.column,
            "id_columns": self.id_columns,
            "value": self.value,
            "rows": self.rows,
            "violations": self.violations,
            "pass_rate": self.pass_rate,
            "pass_threshold": self.pass_threshold,
            "options": self.options,
            "failed_rows_dataset": self.failed_rows_dataset,
            "status": self.status.value
        }