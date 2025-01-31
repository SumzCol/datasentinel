import operator
from typing import Dict, Callable, List

import pandas as pd

import pandas.api.types as pdt

from dataguard.validation.failed_rows_dataset.pandas import PandasFailedRowsDataset
from dataguard.validation.check.row_level_result.rule import Rule
from dataguard.validation.check.row_level_result.utils import evaluate_pass_rate
from dataguard.validation.check.row_level_result.validation_strategy import ValidationStrategy
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


class PandasValidationStrategy(ValidationStrategy):
    def __init__(self):
        self._compute_instructions: Dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {}

    def is_complete(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
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
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[[
                *rule.id_columns,
                *rule.column
            ]]
            return (
                df[df[list(rule.column)].isnull().any(axis=1)]
            )
        self._compute_instructions[rule.key] = _execute

    def has_pattern(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  ~df[rule.column].astype(str).str.match(rule.value, na=False)
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_unique(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[[rule.column]]
            return (
                df[rule.column].value_counts().reset_index().query('count > 1')[[rule.column]]
            )
        self._compute_instructions[rule.key] = _execute

    def are_unique(self, rule: Rule): ## ojo
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[[
                *rule.column
            ]]
            return (
                df[df[list(rule.column)].isnull().any(axis=1)]
            )
        self._compute_instructions[rule.key] = _execute

    def is_greater_than(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column] <= rule.value
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_greater_or_equal_than(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column] < rule.value
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_less_than(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column] >= rule.value
                ]
            )
        self._compute_instructions[rule.key] = _execute
    
    def is_less_or_equal_than(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column] > rule.value
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_equal_than(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column] != rule.value
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_between(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  (df[rule.column] < rule.value[0]) | (df[rule.column] > rule.value[1]) 
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def is_in(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  ~df[rule.column].isin(rule.value)
                ]
            )
        self._compute_instructions[rule.key] = _execute

    def not_in(self, rule: Rule):
        def _execute(df: pd.DataFrame) -> pd.DataFrame:
            df = df[
                [
                    rule.column
                ]
            ]
            return (
                df[
                  df[rule.column].isin(rule.value)
                ]
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

    def validate_data_types(self, df: pd.DataFrame, rules: Dict[str, Rule]) -> bool:
        valid = True
        for key, rule in rules.items():
            dtype = rule.data_type.value
            if  dtype == 1: valid = valid and pdt.is_numeric_dtype(df[rule.column])
            elif dtype == 2: valid = valid and pdt.is_string_dtype(df[rule.column])
            elif dtype in [3,4]: valid = valid and pdt.is_datetime64_any_dtype(df[rule.column])
        return valid

    def compute(self, df: pd.DataFrame, rules: Dict[str, Rule]) -> List[RuleMetric]:
        rows = df.shape[0]
        self._generate_compute_instructions(rules)
        bad_records = self._compute_bad_records(df)

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
                    failed_rows_dataset=PandasFailedRowsDataset(bad_records[rule.key]),
                )
            )

        return rule_metrics

