from pyspark.sql import SparkSession
import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status
from tests.validation.check.row_level_result.utils.numeric_check_tests_cases import (
    is_greater_than_tests_cases_parameterize,
)


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsGreaterThanUnit:
    @pytest.mark.parametrize(
        **is_greater_than_tests_cases_parameterize(pass_outcome=True),
    )
    def test_pass(
        self,
        check: RowLevelResultCheck,
        spark: SparkSession,
        data: list[tuple],
        columns: list[str],
        evaluated_column: str,
        id_columns: list[str],
        rule_value: float | int,
        expected_violations: int,
    ):
        evaluated_rows = len(data)

        df = spark.createDataFrame(data=data, schema=columns)
        result = check.is_greater_than(
            column=evaluated_column, value=rule_value, id_columns=id_columns
        ).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None
        assert result.rule_metrics[0].column == [evaluated_column]
        assert result.rule_metrics[0].id_columns == id_columns

    @pytest.mark.parametrize(
        **is_greater_than_tests_cases_parameterize(pass_outcome=False),
    )
    def test_fail(
        self,
        check: RowLevelResultCheck,
        spark: SparkSession,
        data: list[tuple],
        columns: list[str],
        evaluated_column: str,
        id_columns: list[str],
        rule_value: float | int,
        expected_violations: int,
    ):
        evaluated_rows = len(data)

        df = spark.createDataFrame(data=data, schema=columns)
        result = check.is_greater_than(
            column=evaluated_column, value=rule_value, id_columns=id_columns
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == columns
        assert result.rule_metrics[0].column == [evaluated_column]
        assert result.rule_metrics[0].id_columns == id_columns
