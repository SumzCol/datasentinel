from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
import pytest

from datasentinel.validation.check import RowLevelResultCheck
from datasentinel.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsUniqueUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1,), (2,)],
            [("a",), ("b",)],
            [(1.0,), (2.0,)],
            [(date.today(),), (date.today() + timedelta(days=10),)],
            [(datetime.now(),), (datetime.now() + timedelta(days=10),)],
        ],
    )
    def test_pass(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = spark.createDataFrame(
            data=data,
            schema=[evaluated_column],
        )
        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data",
        [
            [(1,), (1,)],
            [("a",), ("a",)],
            [(1.0,), (1.0,)],
            [(date(2020, 1, 1),), (date(2020, 1, 1),)],
            [(datetime(2020, 1, 1),), (datetime(2020, 1, 1),)],
        ],
    )
    def test_fail(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_rows = len(data)
        expected_violations = 1
        evaluated_column = "col"

        df = spark.createDataFrame(
            data=data,
            schema=[evaluated_column],
        )
        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [evaluated_column]

    @pytest.mark.parametrize(
        "ignore_nulls, result_status",
        [
            (False, Status.FAIL),
            (True, Status.PASS),
        ],
    )
    def test_with_ignoring_nulls_param(
        self,
        spark: SparkSession,
        check: RowLevelResultCheck,
        ignore_nulls: bool,
        result_status: Status,
    ):
        data = [(1,), (None,), (None,)]
        evaluated_column = "col"

        df = spark.createDataFrame(data=data, schema=[evaluated_column])
        result = check.is_unique(column=evaluated_column, ignore_nulls=ignore_nulls).validate(df)

        assert result.status == result_status
