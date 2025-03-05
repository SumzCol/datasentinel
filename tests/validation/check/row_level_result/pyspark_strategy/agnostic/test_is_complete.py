from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsCompleteUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (2, 2)],
            [(1, "a"), (2, "b")],
            [(1, 1.0), (2, 2.0)],
            [(1, date.today()), (2, date.today())],
            [(1, datetime.now()), (2, datetime.now())],
        ],
    )
    def test_pass(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"
        id_columns = ["id"]

        df = spark.createDataFrame(
            data=data,
            schema=[*id_columns, evaluated_column],
        )
        result = check.is_complete(id_columns=id_columns, column=evaluated_column).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data",
        [
            [
                (1, 1),
                (2, None),
            ],
            [
                (1, "a"),
                (2, None),
            ],
            [
                (1, 1.02),
                (2, None),
            ],
            [
                (1, date.today()),
                (2, None),
            ],
            [
                (1, datetime.now()),
                (2, None),
            ],
        ],
    )
    def test_fail(self, spark: SparkSession, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_rows = len(data)
        expected_violations = 1
        evaluated_column = "col"
        id_columns = ["id"]

        df = spark.createDataFrame(
            data=data,
            schema=[*id_columns, evaluated_column],
        )
        result = check.is_complete(id_columns=id_columns, column=evaluated_column).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [
            *id_columns,
            evaluated_column,
        ]
