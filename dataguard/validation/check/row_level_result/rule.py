import enum
import hashlib
from collections import Counter
from datetime import datetime, date
from typing import Tuple, Optional, Any, Dict, List, Set, Callable

from pydantic import Field, model_validator, field_validator
from pydantic.dataclasses import dataclass
from typing_extensions import Self


class CheckDataType(enum.Enum):
    """Accepted data types in checks"""

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
        data_type: Rule data type
        pass_threshold: Rule pass threshold
        options: Rule options
        status: Rule status
    """
    method: str
    data_type: CheckDataType
    pass_threshold: float = 1.0
    value: Optional[int|float|str|datetime|date|List|Callable] = None
    column: Optional[str | List[str]] = None
    id_columns: Optional[List[str]] = None
    options: Optional[Dict[str, Any]] = None
    status: Optional[str] = None

    @field_validator('pass_threshold', mode='after')
    def validate_pass_threshold(cls, pass_threshold: float) -> float:
        if not 0 <= pass_threshold <= 1:
            raise ValueError("The pass threshold should be between 0 and 1")
        return pass_threshold

    @model_validator(mode="after")
    def validate_value(self) -> Self:
        if self.value is None:
            return self
        if isinstance(self.value, List) & (self.data_type == CheckDataType.AGNOSTIC):
            # All values can only be of one data type in a rule
            if len(Counter(map(type, self.value)).keys()) > 1:
                raise ValueError("Data types in rule values are inconsistent")
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
        return (f"Rule(method:{self.method}, column:{self.column}, id_columns:{self.id_columns}, "
                f"value:{self.value}, data_type:{self.data_type}, "
                f"pass_threshold:{self.pass_threshold})")

    def __rshift__(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        rule_dict[self.key] = self
        return rule_dict