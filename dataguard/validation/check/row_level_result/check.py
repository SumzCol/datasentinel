import importlib
import inspect
from collections.abc import Callable
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from dataguard.validation.check.core import (
    AbstractCheck,
    BadArgumentError,
    DataframeType,
    EmptyCheckError,
    UnsupportedDataframeTypeError,
)
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.row_level_result.rule import Rule, RuleDataType
from dataguard.validation.check.row_level_result.utils import (
    are_id_columns_in_rule_columns,
)
from dataguard.validation.check.utils import to_df_if_delta_table

if TYPE_CHECKING:
    from dataguard.validation.check.row_level_result.validation_strategy import (
        ValidationStrategy,
    )


class RowLevelResultCheck(AbstractCheck):
    """Check implementation that returns failed rows."""

    def __init__(self, level: CheckLevel, name: str):
        self._rules: dict[str, Rule] = {}
        super().__init__(level, name)

    @property
    def rules(self) -> list[Rule]:
        """Returns all rules defined in check"""
        return list(self._rules.values())

    def is_complete(self, id_columns: list[str], column: str, pct: float = 1.0) -> Self:
        if are_id_columns_in_rule_columns(id_columns, column):
            raise BadArgumentError("ID columns cannot be evaluated in 'is_complete' rule")
        if not id_columns:
            raise BadArgumentError("ID columns cannot be empty in 'is_complete' rule")
        (
            Rule(
                method="is_complete",
                column=[column],
                id_columns=id_columns,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def are_complete(self, id_columns: list[str], column: list[str], pct: float = 1.0) -> Self:
        if are_id_columns_in_rule_columns(id_columns, column):
            raise BadArgumentError("ID columns cannot be evaluated in 'are_complete' rule")
        (
            Rule(
                method="are_complete",
                column=column,
                id_columns=id_columns,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_unique(self, column: str, pct: float = 1.0, ignore_nulls: bool = False) -> Self:
        (
            Rule(
                method="is_unique",
                column=[column],
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
                options={"ignore_nulls": ignore_nulls},
            )
            >> self._rules
        )
        return self

    def are_unique(self, column: list[str], pct: float = 1.0, ignore_nulls: bool = False) -> Self:
        (
            Rule(
                method="are_unique",
                column=column,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
                options={"ignore_nulls": ignore_nulls},
            )
            >> self._rules
        )
        return self

    def has_pattern(
        self, column: str, value: str, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="has_pattern",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.STRING,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_greater_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_greater_than",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_greater_or_equal_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_greater_or_equal_than",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_less_than(
        self, column: str, value: float, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_less_than",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_less_or_equal_to(
        self, column: str, value: float, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_less_or_equal_to",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_equal_to(
        self, column: str, value: float, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_equal_to",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.NUMERIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_between(
        self,
        column: str,
        value: list[float] | list[int] | list[datetime] | list[date],
        pct: float = 1.0,
        id_columns: list[str] | None = None,
    ) -> Self:
        if len(value) != 2:  # noqa PLR2004
            raise BadArgumentError("Value must be a list containing min and max values")
        (
            Rule(
                method="is_between",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_in(
        self, column: str, value: list, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="is_in",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def not_in(
        self, column: str, value: list, pct: float = 1.0, id_columns: list[str] | None = None
    ) -> Self:
        (
            Rule(
                method="not_in",
                column=[column],
                id_columns=[] if id_columns is None else id_columns,
                value=value,
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pct,
            )
            >> self._rules
        )
        return self

    def is_custom(
        self, fn: Callable, pct: float = 1.0, options: dict[str, Any] | None = None
    ) -> Self:
        if fn is None or not callable(fn):
            raise BadArgumentError("The function must be callable.")

        n_params = len(inspect.signature(fn).parameters)
        if not 0 < n_params <= 2:  # noqa PLR2004
            raise BadArgumentError(
                "The function must have exactly 1 or 2 parameters:\n"
                "1. The first parameter should be the dataframe to be validated.\n"
                "2. The optional second parameter should be a dictionary with the options to be "
                "used inside the function."
            )

        (
            Rule(
                method="is_custom",
                function=fn,
                data_type=RuleDataType.AGNOSTIC,
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
            validation_strategy = importlib.import_module(
                "dataguard.validation.check.row_level_result.pyspark_strategy"
            ).PysparkValidationStrategy()
        elif df_type == DataframeType.PANDAS:
            validation_strategy = importlib.import_module(
                "dataguard.validation.check.row_level_result.pandas_strategy"
            ).PandasValidationStrategy()
        else:
            raise UnsupportedDataframeTypeError(f"Unsupported dataframe type: {df_type.value}")

        validation_strategy: ValidationStrategy
        validation_strategy.validate_data_types(df, self._rules)
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
