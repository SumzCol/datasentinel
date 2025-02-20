import importlib
from datetime import datetime, date
from typing import Any, Dict, List, Callable, Union

from dataguard.validation.check.core import (
    AbstractCheck,
    UnsupportedDataframeTypeError,
    EmptyCheckError,
    DataframeType,
    BadArgumentError,
)
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.row_level_result.utils import (
    are_id_columns_in_rule_columns,
)
from dataguard.validation.check.row_level_result.validation_strategy import (
    ValidationStrategy,
)
from dataguard.validation.check.row_level_result.rule import Rule, CheckDataType
from dataguard.validation.check.utils import to_df_if_delta_table


class RowLevelResultCheck(AbstractCheck):
    """Check implementation that returns failed rows."""

    def __init__(self, level: CheckLevel, name: str):
        self._rules: Dict[str, Rule] = {}
        super().__init__(level, name)

    @property
    def rules(self):
        """Returns all rules defined in check"""
        return list(self._rules.values())

    def is_complete(self, id_columns: List[str], column: str, pct: float = 1.0):
        if are_id_columns_in_rule_columns(id_columns, column):
            raise BadArgumentError(
                "ID columns cannot be evaluated in 'is_complete' rule"
            )
        if not id_columns:
            raise BadArgumentError("ID columns cannot be empty in 'is_complete' rule")
        (
            Rule(
                method="is_complete",
                column=column,
                id_columns=id_columns,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def are_complete(self, id_columns: List[str], column: List[str], pct: float = 1.0):
        if are_id_columns_in_rule_columns(id_columns, column):
            raise BadArgumentError(
                "ID columns cannot be evaluated in 'are_complete' rule"
            )
        (
            Rule(
                method="are_complete",
                column=column,
                id_columns=id_columns,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_unique(self, column: str, pct: float = 1.0, ignore_nulls: bool = False):
        (
            Rule(
                method="is_unique",
                column=column,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
                options={"ignore_nulls": ignore_nulls},
            )
            >> self._rules
        )
        return self

    def are_unique(
        self, column: List[str], pct: float = 1.0, ignore_nulls: bool = False
    ):
        (
            Rule(
                method="are_unique",
                column=column,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
                options={"ignore_nulls": ignore_nulls},
            )
            >> self._rules
        )
        return self

    def has_pattern(
        self, column: str, value: str, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="has_pattern",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.STRING,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_greater_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_greater_than",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_greater_or_equal_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_greater_or_equal_than",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_less_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_less_than",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_less_or_equal_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_less_or_equal_than",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_equal_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_equal_than",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_between(
        self,
        column: str,
        value: Union[List[float], List[int], List[datetime], List[date]],
        pct: float = 1.0,
        id_columns: List[str] = None,
    ):
        if len(value) != 2 or not isinstance(value, List):
            raise BadArgumentError("Value must be a list containing min and max values")
        (
            Rule(
                method="is_between",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_in(
        self, column: str, value: List, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="is_in",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def not_in(
        self, column: str, value: List, pct: float = 1.0, id_columns: List[str] = None
    ):
        (
            Rule(
                method="not_in",
                column=column,
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_custom(self, fn: Callable, pct: float = 1.0, options: Dict[str, Any] = None):
        (
            Rule(
                method="is_custom",
                function=fn,
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pct,
                options=options,
            )
            >> self._rules
        )
        return self

    def validate(self, df: Any) -> CheckResult:
        if len(self.rules) == 0:
            raise EmptyCheckError("No rules were defined in check")
        df = to_df_if_delta_table(df=df)

        df_type = DataframeType.from_df(df)
        if df_type == DataframeType.PYSPARK:
            validation_strategy: ValidationStrategy = importlib.import_module(
                "dataguard.validation.check.row_level_result.pyspark_strategy"
            ).PysparkValidationStrategy()
        elif df_type == DataframeType.PANDAS:
            validation_strategy: ValidationStrategy = importlib.import_module(
                "dataguard.validation.check.row_level_result.pandas_strategy"
            ).PandasValidationStrategy()
        else:
            raise UnsupportedDataframeTypeError(
                f"Unsupported dataframe type: {df_type.value}"
            )

        assert validation_strategy.validate_data_types(df, self._rules), (
            "One or more evaluated columns have a data type incompatible with the defined rules."
        )
        start_time = datetime.now()
        rule_metrics = validation_strategy.compute(df, self._rules)
        end_time = datetime.now()

        return CheckResult(
            name=self.name,
            level=self.level,
            class_name=self.__class__.__name__,
            start_time=start_time,
            end_time=end_time,
            rule_metrics=rule_metrics,
        )
