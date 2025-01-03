import importlib
from datetime import datetime
from typing import Any, Dict, List, Callable


from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.row_level_check.utils import are_id_columns_in_rule_columns
from dataguard.validation.check.row_level_check.validation_strategy import ValidationStrategy
from dataguard.validation.check.row_level_check.rule import Rule, CheckDataType
from dataguard.validation.check.utils import _get_df_type


class RowLevelCheck(AbstractCheck):
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

    def evaluate(self, df: Any) -> CheckResult:
        assert not len(self.rules) == 0, "Check is empty. Try adding some rules?"

        if "delta" in _get_df_type(df):
            df = df.toDF()

        dtype = _get_df_type(df)

        if "pyspark" in dtype:
            validation_strategy: ValidationStrategy = importlib.import_module(
                "dataguard.validation.check.row_level_check.pyspark_strategy"
            ).PysparkValidationStrategy()
        elif "pandas" in dtype:
            validation_strategy: ValidationStrategy = importlib.import_module(
                "dataguard.validation.check.row_level_check.pandas_strategy"
            ).PandasValidationStrategy()
        else:
            raise ValueError(f"Unsupported dataframe type: {dtype}")

        validation_strategy.validate_data_types(df, self._rules)
        start_time = datetime.now()
        rule_metrics = validation_strategy.compute(df, self._rules)
        end_time = datetime.now()

        return CheckResult(
            name=self.name,
            level=self.level,
            type=self.__class__.__name__,
            start_time=start_time,
            end_time=end_time,
            rule_metrics=rule_metrics,
        )
