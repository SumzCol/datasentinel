from datetime import date, datetime

from pyspark.sql import SparkSession
import pytest

from datasentinel.validation.check import RowLevelResultCheck
from datasentinel.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsBetweenUnit:
    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("a",)], ["a", "b"]),
            # Integer
            ([(1,)], [1, 2]),
            # Float
            ([(1.0,)], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 1),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            ([(datetime(2020, 1, 1),)], [datetime(2020, 1, 1), datetime(2020, 1, 2)]),
        ],
    )
    def test_pass(
        self, check: RowLevelResultCheck, spark: SparkSession, data: list[tuple], value: list
    ) -> None:
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = spark.createDataFrame(data=data, schema=[evaluated_column])
        result = check.is_between(
            column=evaluated_column, lower_bound=value[0], upper_bound=value[1]
        ).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data, schema, id_columns, value",
        [
            # String
            ([("c",)], ["col"], [], ["a", "b"]),
            (
                [(1, "c")],
                ["id", "col"],
                ["id"],
                ["a", "b"],
            ),
            # Int
            ([(3,)], ["col"], [], [1, 2]),
            (
                [(1, 0)],
                ["id", "col"],
                ["id"],
                [1, 2],
            ),
            # Float
            ([(2.0000001,)], ["col"], [], [1.0, 2.0]),
            (
                [(1, 0.9999999)],
                ["id", "col"],
                ["id"],
                [1.0, 2.0],
            ),
            # Date
            ([(date(2019, 12, 31),)], ["col"], [], [date(2020, 1, 1), date(2020, 1, 2)]),
            (
                [(1, date(2020, 1, 3))],
                ["id", "col"],
                ["id"],
                [date(2020, 1, 1), date(2020, 1, 2)],
            ),
            # Timestamp
            (
                [(datetime(2020, 12, 31),)],
                ["col"],
                [],
                [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            ),
            (
                [(1, datetime(2020, 1, 3))],
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

        df = spark.createDataFrame(data=data, schema=schema)
        result = check.is_between(
            column="col",
            id_columns=id_columns,
            lower_bound=value[0],
            upper_bound=value[1],
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == schema
