import enum
import hashlib
from collections import Counter
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from pydantic import field_validator, model_validator
from pydantic.dataclasses import dataclass
from typing_extensions import Self


class RuleDataType(enum.Enum):
    """Accepted data types in rule"""

    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIMESTAMP = 4


@dataclass(frozen=True)
class Rule:
    """Predicate definition holder

    Attributes:
        method: Rule method name (e.g. is_complete)
        column: Column or columns to evaluate
        id_columns: ID columns used to identify failed rows if the check generates them
        value: Rule value argument
        function: Rule function if a custom function is used
        data_type: Rule data type
        pass_threshold: Rule pass threshold
        options: Rule options
        status: Rule status
    """

    method: str
    data_type: RuleDataType
    pass_threshold: float = 1.0
    value: int | float | str | datetime | date | list | None = None
    function: Callable | None = None
    column: str | list[str] | None = None
    id_columns: list[str] | None = None
    options: dict[str, Any] | None = None
    status: str | None = None

    @field_validator("pass_threshold", mode="after")
    def validate_pass_threshold(cls, pass_threshold: float) -> float:
        if not 0 <= pass_threshold <= 1:
            raise ValueError("The pass threshold should be between 0 and 1")
        return pass_threshold

    @model_validator(mode="after")
    def validate_value(self) -> Self:
        if self.value is None:
            return self
        if isinstance(self.value, list) & (self.data_type == RuleDataType.AGNOSTIC):
            # All values can only be of one data type in a rule
            if len(Counter(map(type, self.value)).keys()) > 1:
                raise ValueError("Data types in rule values are inconsistent")
        if self.method == "is_custom" and self.function is None:
            raise ValueError("When 'is_custom' method is used, a function must be provided")
        return self

    @property
    def key(self):
        """blake2s hash of the rule, made of method, column, value, options and coverage"""
        return (
            hashlib.blake2s(
                bytes(
                    f"{self.method}{self.column}{self.value}{self.options}{self.pass_threshold}",
                    "utf-8",
                )
            )
            .hexdigest()
            .upper()
        )

    def __repr__(self):
        return (
            f"Rule(method:{self.method}, column:{self.column}, id_columns:{self.id_columns}, "
            f"value:{self.value}, data_type:{self.data_type}, "
            f"pass_threshold:{self.pass_threshold})"
        )

    def __rshift__(self, rule_dict: dict[str, Any]) -> dict[str, Any]:
        rule_dict[self.key] = self
        return rule_dict
