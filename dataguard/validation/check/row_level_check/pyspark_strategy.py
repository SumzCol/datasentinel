import operator
from typing import Callable, Dict, List, Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from dataguard.validation.bad_records.spark_bad_records_dataset import SparkBadRecordsDataset
from dataguard.validation.check.row_level_check.utils import value
from dataguard.validation.check.row_level_check.validation_strategy import ValidationStrategy
from dataguard.validation.check.row_level_check.rule import Rule
from dataguard.validation.rule.metric import RuleMetric


class PysparkValidationStrategy(ValidationStrategy):
    def __init__(self):
        """Determine the computational options for Rules"""
        self._compute_instructions: Dict[str, Callable[[DataFrame], DataFrame]] = {}

    def is_complete(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe
                .select(
                    *rule.id_columns,
                    rule.column,
                )
                .where(
                    F.col(f"{rule.column}").isNull()
                )
            )
        self._compute_instructions[rule.key] = _execute

    def are_complete(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe
                .select(
                    *rule.id_columns,
                    *rule.column,
                )
                .where(
                   " OR ".join(
                       [f"{column} IS NULL" for column in rule.column]
                   )
                )
            )
        self._compute_instructions[rule.key] = _execute

    def is_unique(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe
                .select(
                    rule.column,
                )
                .groupBy(rule.column)
                .count()
                .where(F.col("count") > 1)
                .select(
                    rule.column
                )
            )
        self._compute_instructions[rule.key] = _execute

    def are_unique(self, rule: Rule):
        def _execute(dataframe: DataFrame) -> DataFrame:
            return (
                dataframe
                .select(
                    *rule.column,
                )
                .groupBy(rule.column)
                .count()
                .where(F.col("count") > 1)
                .select(
                    *rule.column
                )
            )
        self._compute_instructions[rule.key] = _execute

    def is_custom(self, rule: Rule):
        def _execute(dataframe: DataFrame):
            computed_frame = rule.value(dataframe, rule.options)
            assert "pyspark" in str(
                type(computed_frame)
            ), "Custom function does not return a PySpark DataFrame"
            assert (
                len(computed_frame.columns) >= 1
            ), "Custom function should retun at least one column"
            return computed_frame

        self._compute_instructions[rule.key] = _execute

    def _generate_compute_instructions(
            self,
            rules: Dict[str, Rule]
    ) -> None:
        for k, v in rules.items():
            operator.methodcaller(v.name, v)(self)

    def _compute_bad_records(
            self,
            dataframe: DataFrame,
    ) -> Dict[str, DataFrame]:
        """Compute rules throught spark transform"""

        return {
            k: compute_instruction(dataframe)  # type: ignore
            for k, compute_instruction in self._compute_instructions.items()
        }

    def validate_data_types(self, dataframe: DataFrame, rules: Dict[str, Rule]) -> bool:
        """
        Validate the datatype of each column according to the CheckDataType of the rule's method
        """
        return True

    def compute(self, dataframe: DataFrame, rules: Dict[str, Rule]) -> List[RuleMetric]:
        """Compute and returns calculated rule metrics"""
        rows = dataframe.count()
        self._generate_compute_instructions(rules)
        bad_records = self._compute_bad_records(dataframe)

        rule_metrics = []
        for index, (hash_key, rule) in enumerate(rules.items(), 1):
            bad_records_count = bad_records[hash_key].count()
            pass_rate = self._evaluate_pass_rate(rows, bad_records_count)
            rule_metrics.append(
                RuleMetric(
                    id=index,
                    rule=rule.name,
                    column=rule.column,
                    value=value(rule),
                    rows=rows,
                    violations=bad_records_count,
                    pass_rate=pass_rate,
                    pass_threshold=rule.pass_threshold,
                    options=rule.options,
                    bad_records=SparkBadRecordsDataset(bad_records[rule.key]),
                )
            )

        return rule_metrics