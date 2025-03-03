import pytest
from pyspark.sql import SparkSession

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
class TestHasPatternUnit:
    def test_pass(self, check: RowLevelResultCheck, spark: SparkSession):
        data = [("spark",)]
        df = spark.createDataFrame(data=data, schema=["col"])
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        result = check.has_pattern(column=evaluated_column, value=r"^s.+k$").validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations

    @pytest.mark.parametrize(
        "data, schema, id_columns",
        [
            ([("pandas",)], ["col"], []),
            ([("pandas",)], ["col"], ["col"]),
            ([(1, "pandas")], ["id", "col"], ["id"]),
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
        df = spark.createDataFrame(data=data, schema=schema)
        evaluated_rows = len(data)
        evaluated_column = "col"
        expected_violations = 1

        result = check.has_pattern(
            column=evaluated_column, value=r"^s.+k$", id_columns=id_columns
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == schema
