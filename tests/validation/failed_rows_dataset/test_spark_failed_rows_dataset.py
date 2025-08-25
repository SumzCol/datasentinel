import json

from pyspark.sql import DataFrame, SparkSession
import pytest

from datasentinel.validation.failed_rows_dataset.core import FailedRowsDatasetError
from datasentinel.validation.failed_rows_dataset.spark import SparkFailedRowsDataset


@pytest.fixture(scope="class")
def spark_df(spark: SparkSession):
    return spark.createDataFrame(data=[(1, "a"), (2, "b")], schema=["id", "name"])


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestSparkFailedRowsDatasetUnit:
    def test_data_property(self, spark_df: DataFrame):
        spark_failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        assert spark_failed_rows_dataset.data == spark_df

    def test_count(self, spark_df: DataFrame):
        expected_count = spark_df.count()
        failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        assert failed_rows_dataset.count() == expected_count

    @pytest.mark.parametrize(
        "limit",
        [None, 1, 2, 3],
    )
    def test_to_dict(self, limit: int | None, spark_df: DataFrame):
        expected_result = (
            [row.asDict() for row in spark_df.limit(limit).collect()]
            if limit is not None
            else [row.asDict() for row in spark_df.collect()]
        )

        failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        assert failed_rows_dataset.to_dict(limit=limit) == expected_result

    @pytest.mark.parametrize(
        "limit",
        [-1, 0],
    )
    def test_error_to_dict_with_less_than_zero_limit(self, limit: int, spark_df: DataFrame):
        failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        with pytest.raises(FailedRowsDatasetError, match="Limit must be greater than 0"):
            failed_rows_dataset.to_dict(limit=limit)

    @pytest.mark.parametrize(
        "limit",
        [None, 1, 2, 3],
    )
    def test_to_json(self, limit: int | None, spark_df: DataFrame):
        expected_result = json.dumps(
            [row.asDict() for row in spark_df.limit(limit).collect()]
            if limit is not None
            else [row.asDict() for row in spark_df.collect()]
        )
        failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        assert failed_rows_dataset.to_json(limit=limit) == expected_result

    @pytest.mark.parametrize(
        "limit",
        [-1, 0],
    )
    def test_error_to_json_with_less_than_zero_limit(self, limit: int, spark_df: DataFrame):
        failed_rows_dataset = SparkFailedRowsDataset(data=spark_df)

        with pytest.raises(FailedRowsDatasetError, match="Limit must be greater than 0"):
            failed_rows_dataset.to_json(limit=limit)
