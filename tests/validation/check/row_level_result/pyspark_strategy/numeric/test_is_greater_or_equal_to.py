import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsGreaterOrEqualThanUnit:
    def test_pass(self, check: RowLevelResultCheck, spark: SparkSession):
        data = [(1.0,), (2.0,)]
        df = spark.createDataFrame(data=data, schema=["col"])
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        result = check.is_greater_or_equal_to(column=evaluated_column, value=1.0).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data, schema, id_columns",
        [
            ([(0.9999999999,)], ["col"], []),
            ([(0.9999999999,)], ["col"], ["col"]),
            ([(1, 0.98)], ["id", "col"], ["id"]),
        ],
    )
    def test_fail_with_and_without_id_columns(
        self,
        check: RowLevelResultCheck,
        spark: SparkSession,
        data: list[tuple],
        schema: list[str],
        id_columns: list[str],
    ):
        evaluated_rows = len(data)
        evaluated_column = "col"
        expected_violations = 1

        df = spark.createDataFrame(data=data, schema=schema)
        result = check.is_greater_or_equal_to(
            column=evaluated_column, value=1.0, id_columns=id_columns
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == schema
