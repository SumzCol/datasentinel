import operator
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType

from dataguard.validation.check.row_level_result.rule import Rule, RuleDataType
from dataguard.validation.check.row_level_result.utils import evaluate_pass_rate
from dataguard.validation.check.row_level_result.validation_strategy import (
    ValidationStrategy,
)
from dataguard.validation.failed_rows_dataset.spark import SparkFailedRowsDataset
from dataguard.validation.rule.metric import RuleMetric

if TYPE_CHECKING:
    from collections.abc import Callable


class PysparkValidationStrategy(ValidationStrategy):
    def __init__(self):
        """Determine the computational options for Rules"""
        self._compute_instructions: dict[str, Callable[[DataFrame], DataFrame]] = {}

    def is_complete(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                *rule.id_columns,
                rule.column,
            ).where(F.col(f"{rule.column}").isNull())

        self._compute_instructions[rule.key] = _execute

    def are_complete(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                *rule.id_columns,
                *rule.column,
            ).where(" OR ".join([f"{column} IS NULL" for column in rule.column]))

        self._compute_instructions[rule.key] = _execute

    def is_unique(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe.select(
                    rule.column,
                )
                .groupBy(rule.column)
                .count()
                .where(F.col("count") > 1)
                .select(rule.column)
            )

        self._compute_instructions[rule.key] = _execute

    def are_unique(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe.select(
                    *rule.column,
                )
                .groupBy(rule.column)
                .count()
                .where(F.col("count") > 1)
                .select(*rule.column)
            )

        self._compute_instructions[rule.key] = _execute

    def has_pattern(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(~F.col(rule.column).rlike(rule.value))

        self._compute_instructions[rule.key] = _execute

    def is_greater_than(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} <= {rule.value}")

        self._compute_instructions[rule.key] = _execute

    def is_greater_or_equal_than(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} < {rule.value}")

        self._compute_instructions[rule.key] = _execute

    def is_less_than(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} >= {rule.value}")

        self._compute_instructions[rule.key] = _execute

    def is_less_or_equal_than(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} > {rule.value}")

        self._compute_instructions[rule.key] = _execute

    def is_equal_than(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} != {rule.value}")

        self._compute_instructions[rule.key] = _execute

    def is_between(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(f"{rule.column} < {rule.value[0]} or {rule.column} > {rule.value[1]}")

        self._compute_instructions[rule.key] = _execute

    def is_in(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(~F.col(rule.column).isin(rule.value))

        self._compute_instructions[rule.key] = _execute

    def not_in(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return dataframe.select(
                rule.column,
            ).filter(F.col(rule.column).isin(rule.value))

        self._compute_instructions[rule.key] = _execute

    def is_custom(self, rule: Rule):
        def _execute(dataframe: DataFrame):
            computed_frame = rule.function(dataframe, rule.options)
            if "pyspark" not in str(type(computed_frame)):
                raise ValueError("Custom function does not return a PySpark DataFrame")
            if not len(computed_frame.columns) >= 1:
                raise ValueError("Custom function should return at least one column")
            return computed_frame

        self._compute_instructions[rule.key] = _execute

    def _generate_compute_instructions(self, rules: dict[str, Rule]) -> None:
        for k, v in rules.items():
            operator.methodcaller(v.method, v)(self)

    def _compute_bad_records(
        self,
        dataframe: DataFrame,
    ) -> dict[str, DataFrame]:
        """Compute rules through spark transform"""

        return {
            k: compute_instruction(dataframe)  # type: ignore
            for k, compute_instruction in self._compute_instructions.items()
        }

    def validate_data_types(self, df: DataFrame, rules: dict[str, Rule]) -> bool:
        """
        Validate the datatype of each column according to the CheckDataType of the rule's method
        """
        valid = True
        for key, rule in rules.items():
            if rule.data_type == RuleDataType.NUMERIC:
                valid = valid and isinstance(df.schema[rule.column].dataType, NumericType)
            elif rule.data_type == RuleDataType.STRING:
                valid = valid and isinstance(df.schema[rule.column].dataType, StringType)
            elif rule.data_type == RuleDataType.DATE:
                valid = valid and isinstance(df.schema[rule.column].dataType, DateType)
            elif rule.data_type == RuleDataType.TIMESTAMP:
                valid = valid and isinstance(df.schema[rule.column].dataType, TimestampType)
        return valid

    def compute(self, df: DataFrame, rules: dict[str, Rule]) -> list[RuleMetric]:
        """Compute and returns calculated rule metrics"""
        rows = df.count()
        self._generate_compute_instructions(rules)
        bad_records = self._compute_bad_records(df)

        rule_metrics = []
        for index, (hash_key, rule) in enumerate(rules.items(), 1):
            bad_records_count = bad_records[hash_key].count()
            pass_rate = evaluate_pass_rate(rows, bad_records_count)
            rule_metrics.append(
                RuleMetric(
                    id=index,
                    rule=rule.method,
                    column=rule.column,
                    id_columns=rule.id_columns,
                    value=rule.value,
                    function=rule.function,
                    rows=rows,
                    violations=bad_records_count,
                    pass_rate=pass_rate,
                    pass_threshold=rule.pass_threshold,
                    options=rule.options,
                    failed_rows_dataset=SparkFailedRowsDataset(bad_records[rule.key]),
                )
            )

        return rule_metrics
