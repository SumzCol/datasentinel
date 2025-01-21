import importlib
from datetime import datetime
from typing import Any, Dict, List, Callable

from dataguard.validation.check.core import (
    AbstractCheck,
    UnsupportedDataframeTypeError,
    EmptyCheckError,
)
from dataguard.validation.check.df_type import DataframeType
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.row_level_result.utils import are_id_columns_in_rule_columns
from dataguard.validation.check.row_level_result.validation_strategy import ValidationStrategy
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
            raise ValueError("ID columns cannot be evaluated in 'is_complete' rule")
        Rule("is_complete", column, id_columns, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rules
        return self

    def are_complete(self, id_columns: List[str], column: List[str], pct: float = 1.0):
        if are_id_columns_in_rule_columns(id_columns, column):
            raise ValueError("ID columns cannot be evaluated in 'are_complete' rule")
        Rule(
            "are_complete", column, id_columns, "N/A", CheckDataType.AGNOSTIC, pct
        ) >> self._rules
        return self

    def is_unique(self, column: str, pct: float = 1.0):
        Rule("is_unique", column, None, "N/A", CheckDataType.AGNOSTIC, pct) >> self._rules
        return self

    def are_unique(self, column: List[str], pct: float = 1.0):
        Rule(
            "are_unique", column, None, "N/A", CheckDataType.AGNOSTIC, pct
        ) >> self._rules
        return self

    def is_custom(
            self,
            fn: Callable,
            pct: float = 1.0,
            options: Dict[str, Any] = None
    ):
        assert isinstance(
            fn, Callable
        ), "Please provide a Callable/Function for validation"

        Rule("is_custom", None, None, fn, CheckDataType.AGNOSTIC, pct, options) >> self._rules
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
            raise UnsupportedDataframeTypeError(f"Unsupported dataframe type: {df_type.value}")

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
