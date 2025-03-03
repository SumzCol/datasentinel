from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
class TestAreUniqueUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (2, 2)],
            [("a", "a"), ("b", "b")],
            [(1.0, 1.0), (2.0, 2.0)],
            [(date(2020, 1, 1), date(2020, 1, 1)), (date(2020, 1, 2), date(2020, 1, 2))],
            [
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                (datetime(2020, 1, 2), datetime(2020, 1, 2)),
            ],
        ],
    )
    def test_pass(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        df = spark.createDataFrame(
            data=data,
            schema=["id", "id2"],
        )
        evaluated_rows = len(data)
        expected_violations = 0

        result = check.are_unique(column=["id", "id2"]).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.to_dict() == []

    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (1, 1)],
            [("a", "a"), ("a", "a")],
            [(1.0, 1.0), (1.0, 1.0)],
            [(date(2020, 1, 1), date(2020, 1, 1)), (date(2020, 1, 1), date(2020, 1, 1))],
            [
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
            ],
        ],
    )
    def test_fail(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        df = spark.createDataFrame(
            data=data,
            schema=["id", "id2"],
        )
        evaluated_rows = len(data)
        expected_violations = 1

        result = check.are_unique(column=["id", "id2"]).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == ["id", "id2"]

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
        data = [(1, 1), (1, None), (None, 1), (None, None), (None, None)]
        df = spark.createDataFrame(data=data, schema=["id", "id2"])
        result = check.are_unique(column=["id", "id2"], ignore_nulls=ignore_nulls).validate(df)
        assert result.status == result_status
