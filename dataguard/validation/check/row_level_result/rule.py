import enum
import hashlib
from collections import Counter
from dataclasses import dataclass
from typing import Union, List, Tuple, Optional, Any, Dict


class CheckDataType(enum.Enum):
    """Accepted data types in checks"""

    AGNOSTIC = 0
    NUMERIC = 1
    STRING = 2
    DATE = 3
    TIMESTAMP = 4
    DUO = 5


@dataclass
class Rule:
    """Predicate definition holder"""

    method: str
    column: Union[str, List[str], Tuple[str, ...], None]
    id_columns: Union[List[str], Tuple[str, ...], None]
    value: Optional[Any]
    data_type: CheckDataType
    pass_threshold: float = 1.0
    options: Union[Dict[str, Any], None] = None
    status: Union[str, None] = None
    name: str = None

    @property
    def key(self):
        """blake2s hash of the rule, made of method, column, value, options and coverage"""
        return (
            hashlib.blake2s(
                bytes(
                    f"{self.name}{self.column}{self.value}{self.options}{self.pass_threshold}",
                    "utf-8",
                )
            )
            .hexdigest()
            .upper()
        )

    def __post_init__(self):
        if (self.pass_threshold <= 0) or (self.pass_threshold > 1):
            raise ValueError("Coverage should be between 0 and 1")

        if isinstance(self.column, List):
            self.column = tuple(self.column)

        if isinstance(self.id_columns, List):
            self.id_columns = tuple(self.id_columns)

        if isinstance(self.value, List):
            self.value = tuple(self.value)

        if isinstance(self.value, Tuple) & (self.data_type == CheckDataType.AGNOSTIC):
            # All values can only be of one data type in a rule
            if len(Counter(map(type, self.value)).keys()) > 1:
                raise ValueError("Data types in rule values are inconsistent")

        self.name = self.method if self.name is None else self.name

    def __repr__(self):
        return (f"Rule(method:{self.method}, column:{self.column}, id_columns:{self.id_columns}, "
                f"value:{self.value}, data_type:{self.data_type}, "
                f"pass_threshold:{self.pass_threshold})")

    def __rshift__(self, rule_dict: Dict[str, Any]) -> Dict[str, Any]:
        rule_dict[self.key] = self
        return rule_dict