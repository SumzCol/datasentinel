from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.status import Status


class TestIsComplete:
    @pytest.mark.parametrize(
        "evaluated_column",
        ["string_col", "integer_col", "float_col", "date_col", "timestamp_col"],
        ids=("string", "integer", "float", "date", "timestamp"),
    )
    def test_pass_with_columns_of_various_data_types(
        self, spark: SparkSession, evaluated_column: str
    ):
        df = spark.createDataFrame(
            data=[
                (1, "a", 1, 1.02, date.today(), datetime.today()),
                (2, "b", 2, 2.02, date.today(), datetime.today()),
                (3, "c", 3, 0.1, date.today(), datetime.today()),
                (4, "d", 4, 4.02, date.today(), datetime.today()),
            ],
            schema=[
                "id",
                "string_col",
                "integer_col",
                "float_col",
                "date_col",
                "timestamp_col",
            ],
        )
        evaluated_rows = df.count()
        expected_violations = 0

        result = (
            RowLevelResultCheck(
                level=CheckLevel.WARNING,
                name="test",
            )
            .is_complete(id_columns=["id"], column="string_col")
            .validate(df)
        )

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.to_dict() == []

    @pytest.mark.parametrize(
        "evaluated_column",
        ["string_col", "integer_col", "float_col", "date_col", "timestamp_col"],
        ids=("string", "integer", "float", "date", "timestamp"),
    )
    def test_fail_with_columns_of_various_data_types(
        self, spark: SparkSession, evaluated_column: str
    ):
        df = spark.createDataFrame(
            data=[
                (1, "a", 1, 1.02, date.today(), datetime.today()),
                (2, None, None, None, None, None),
                (3, "c", 3, 0.1, date.today(), datetime.today()),
                (4, None, None, None, None, None),
            ],
            schema=[
                "id",
                "string_col",
                "integer_col",
                "float_col",
                "date_col",
                "timestamp_col",
            ],
        )
        evaluated_rows = df.count()
        expected_violations = 2

        result = (
            RowLevelResultCheck(
                level=CheckLevel.WARNING,
                name="test",
            )
            .is_complete(id_columns=["id"], column=evaluated_column)
            .validate(df)
        )

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.to_dict() == [
            {"id": 2, evaluated_column: None},
            {"id": 4, evaluated_column: None},
        ]
