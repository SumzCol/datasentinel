from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
)
import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestIsCustomUnit:
    def test_pass_without_options(self, check: RowLevelResultCheck, spark: SparkSession):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe.where(F.col(evaluated_column) == "a")

        data = [("b",), ("c",)]
        expected_rows = len(data)
        expected_violations = 0

        df = spark.createDataFrame(data, schema=[evaluated_column])
        result = check.is_custom(fn).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    def test_pass_with_options(self, check: RowLevelResultCheck, spark: SparkSession):
        evaluated_column = "col"

        def fn(dataframe: DataFrame, options: dict) -> DataFrame:
            return dataframe.where(F.col(evaluated_column) == options["value"])

        data = [("b",), ("c",)]
        expected_rows = len(data)
        expected_violations = 0

        df = spark.createDataFrame(data, schema=[evaluated_column])
        result = check.is_custom(fn=fn, options={"value": "a"}).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    def test_fail_without_options(self, check: RowLevelResultCheck, spark: SparkSession):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe.where(F.col(evaluated_column) == "a")

        data = [("a",), ("b",)]
        expected_rows = len(data)
        expected_violations = 1

        df = spark.createDataFrame(data, schema=[evaluated_column])
        result = check.is_custom(fn).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [evaluated_column]

    def test_fail_with_options(self, check: RowLevelResultCheck, spark: SparkSession):
        evaluated_column = "col"

        def fn(dataframe: DataFrame, options: dict) -> DataFrame:
            return dataframe.where(F.col(evaluated_column) == options["value"])

        data = [("a",), ("b",)]
        expected_rows = len(data)
        expected_violations = 1

        df = spark.createDataFrame(data, schema=[evaluated_column])
        result = check.is_custom(fn=fn, options={"value": "a"}).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [evaluated_column]

    def test_error_on_fn_that_does_not_return_a_spark_dataframe(
        self, check: RowLevelResultCheck, spark: SparkSession
    ):
        def fn(dataframe: DataFrame) -> bool:
            return True

        data = [("a",), ("b",)]
        schema = ["col"]

        df = spark.createDataFrame(data, schema)
        with pytest.raises(ValueError):
            check.is_custom(fn=fn).validate(df)
