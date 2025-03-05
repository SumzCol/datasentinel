from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsInUnit:
    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("c",)], ["a", "b"]),
            # Integer
            ([(3,)], [1, 2]),
            # Float
            ([(3.0,)], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 3),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            ([(datetime(2020, 1, 3),)], [datetime(2020, 1, 1), datetime(2020, 1, 2)]),
        ],
    )
    def test_pass(
        self, check: RowLevelResultCheck, spark: SparkSession, data: list[tuple], value: list
    ) -> None:
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = spark.createDataFrame(data=data, schema=[evaluated_column])
        result = check.not_in(column=evaluated_column, value=value).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data, schema, id_columns, value",
        [
            # String
            ([("a",)], ["col"], [], ["a", "b"]),
            (
                [(1, "b")],
                ["id", "col"],
                ["id"],
                ["a", "b"],
            ),
            # Int
            ([(1,)], ["col"], [], [1, 2]),
            ([(1, 2)], ["id", "col"], ["id"], [1, 2]),
            # Float
            ([(1.0,)], ["col"], [], [1.0, 2.0]),
            ([(1, 2.0)], ["id", "col"], ["id"], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 2),)], ["col"], [], [date(2020, 1, 1), date(2020, 1, 2)]),
            (
                [(1, date(2020, 1, 2))],
                ["id", "col"],
                ["id"],
                [date(2020, 1, 1), date(2020, 1, 2)],
            ),
            # Timestamp
            (
                [(datetime(2020, 1, 2),)],
                ["col"],
                [],
                [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            ),
            (
                [(1, datetime(2020, 1, 2))],
                ["id", "col"],
                ["id"],
                [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            ),
        ],
    )
    def test_fail_with_and_without_id_columns(
        self,
        check: RowLevelResultCheck,
        spark: SparkSession,
        data: list[tuple],
        schema: list[str],
        id_columns: list[str],
        value: list,
    ) -> None:
        evaluated_rows = len(data)
        expected_violations = 1
        evaluated_column = "col"

        df = spark.createDataFrame(data=data, schema=schema)
        result = check.not_in(
            column=evaluated_column, id_columns=id_columns, value=value
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == schema
