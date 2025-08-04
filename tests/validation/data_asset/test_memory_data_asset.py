from pandas import DataFrame
from pyspark.sql import SparkSession
import pytest

from dataguard.validation.data_asset.core import DataAssetError
from dataguard.validation.data_asset.memory import MemoryDataAsset


@pytest.mark.unit
class TestMemoryDataAssetUnit:
    def test_name_property(self):
        expected_name = "test_name"
        data_asset = MemoryDataAsset(name=expected_name, data=None)

        assert data_asset.name == expected_name

    @pytest.mark.parametrize("expected_schema", [None, "test_schema"])
    def test_schema_property(self, expected_schema: str | None):
        data_asset = MemoryDataAsset(name="test_name", data=None, schema=expected_schema)

        assert data_asset.schema == expected_schema

    def test_error_on_load_with_empty_data(self):
        data_asset = MemoryDataAsset(name="test_name", data=None)

        with pytest.raises(
            DataAssetError, match="Data for MemoryDataAsset has not been saved yet."
        ):
            data_asset.load()

    def test_pandas_df_as_data(self):
        df = DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        data_asset = MemoryDataAsset(name="test_name", data=df)

        assert data_asset.load().equals(df)

    def test_spark_df_as_data(self, spark: SparkSession):
        df = spark.createDataFrame(data=[(1, 2), (3, 4)], schema=["col1", "col2"])
        data_asset = MemoryDataAsset(name="test_name", data=df)

        assert data_asset.load() == df
