import operator
from typing import Dict, Callable, List

import pandas as pd

from dataguard.validation.bad_records.pandas_bad_records_dataset import PandasBadRecordsDataset
from dataguard.validation.check.row_level_check.rule import Rule
from dataguard.validation.check.row_level_check.utils import evaluate_pass_rate
from dataguard.validation.check.row_level_check.validation_strategy import ValidationStrategy
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


class PandasValidationStrategy(ValidationStrategy):
    def __init__(self):
        """Determine the computational options for Rules"""
        self._compute_instructions: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {}

    def is_complete(self, rule: Rule):
        def _execute(dataframe: pd.DataFrame) -> pd.DataFrame:
            df = dataframe[
                [
                    *rule.id_columns,
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column].isna()
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def are_complete(self, rule: Rule):
        def _execute(dataframe: pd.DataFrame) -> pd.DataFrame:
            df = dataframe[[
                *rule.id_columns,
                *rule.column
            ]]
            return (
                df[df[list(rule.column)].isnull().any(axis=1)]
            )
        self._compute_instructions[rule.key] = _execute

    def _generate_compute_instructions(
            self,
            rules: Dict[str, Rule]
    ) -> None:
        for v in rules.values():
            operator.methodcaller(v.name, v)(self)

    def _compute_bad_records(
            self,
            dataframe: pd.DataFrame,
    ) -> Dict[str, pd.DataFrame]:
        """Compute bad records"""
        return {
            k: compute_instruction(dataframe)  # type: ignore
            for k, compute_instruction in self._compute_instructions.items()
        }

    def validate_data_types(self, dataframe: pd.DataFrame, rules: Dict[str, Rule]) -> bool:
        return True

    def compute(self, dataframe: pd.DataFrame, rules: Dict[str, Rule]) -> List[RuleMetric]:
        rows = dataframe.shape[0]
        self._generate_compute_instructions(rules)
        bad_records = self._compute_bad_records(dataframe)

        rule_metrics = []
        for index, (hash_key, rule) in enumerate(rules.items(), 1):
            bad_records_count = bad_records[hash_key].shape[0]
            pass_rate = evaluate_pass_rate(rows, bad_records_count)
            rule_metrics.append(
                RuleMetric(
                    id=index,
                    rule=rule.name,
                    column=rule.column,
                    value=rule.value,
                    rows=rows,
                    violations=bad_records_count,
                    pass_rate=pass_rate,
                    pass_threshold=rule.pass_threshold,
                    options=rule.options,
                    bad_records=PandasBadRecordsDataset(bad_records[rule.key]),
                )
            )

        return rule_metrics

