from pathlib import Path
from typing import Literal
from unittest.mock import patch

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataguard.store.utils.deltatable_appender import DeltaTableAppender, DeltaTableAppenderError


@pytest.fixture
def sample_dataframe(spark: SparkSession):
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["Name", "Id"]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="function")
def spark_schema(spark: SparkSession):
    schema_name = "test_schema"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    yield schema_name
    tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
    for row in tables:
        table_name = row.tableName
        spark.sql(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
    spark.sql(f"DROP DATABASE IF EXISTS {schema_name}")


@pytest.mark.unit
@pytest.mark.slow
@pytest.mark.pyspark
class TestDeltaTableAppenderUnit:
    @pytest.mark.parametrize(
        "dataset_type, table, schema, external_path, expected_is_external, expected_full_path",
        [
            (
                "file",
                "test_table",
                "s3a://test_schema",
                None,
                False,
                "s3a://test_schema/test_table",
            ),
            (
                "file",
                "test_table",
                "s3a://test_schema/",
                None,
                False,
                "s3a://test_schema/test_table",
            ),
            (
                "file",
                "test_table",
                "s3a://test_schema",
                "s3a://test_schema/test_table",
                False,
                "s3a://test_schema/test_table",
            ),
            ("table", "test_table", "catalog.schema", None, False, "catalog.schema.test_table"),
            (
                "table",
                "test_table",
                "catalog.schema",
                "s3a://test_schema/test_table",
                True,
                "catalog.schema.test_table",
            ),
        ],
        ids=[
            "file_dataset_type",
            "file_dataset_type_with_schema_ending_with_slash",
            "file_dataset_type_with_external_path_defined",
            "table_dataset_type",
            "table_dataset_type_with_external_path_defined",
        ],
    )
    def test_success_initialization(
        self,
        dataset_type: Literal["file", "table"],
        table: str,
        schema: str,
        external_path: str | None,
        expected_is_external: bool,
        expected_full_path: str,
    ):
        appender = DeltaTableAppender(
            table=table,
            schema=schema,
            dataset_type=dataset_type,
            external_path=external_path,
        )

        assert appender.table == table
        assert appender.schema == schema.rstrip("/")
        assert appender.full_table_path == expected_full_path
        assert appender.is_external_table == expected_is_external

    def test_error_on_invalid_dataset_type(self):
        with pytest.raises(DeltaTableAppenderError):
            DeltaTableAppender(
                table="test_table", schema="s3a://test_schema", dataset_type="invalid"
            )

    @pytest.mark.parametrize(
        "schema",
        [
            "catalog.schema.additional",
        ],
    )
    def test_error_on_invalid_schema_with_table_dataset_type(self, schema: str):
        with pytest.raises(DeltaTableAppenderError):
            DeltaTableAppender(table="test_table", schema=schema, dataset_type="table")

    def test_exists_method_with_table_as_dataset_type(
        self, spark_schema: str, sample_dataframe: DataFrame
    ):
        table_name = "test_table"
        appender = DeltaTableAppender(
            table=table_name,
            schema=spark_schema,
            dataset_type="table",
        )
        assert not appender.exists()

        sample_dataframe.write.saveAsTable(
            f"{spark_schema}.{table_name}",
            mode="append",
            format="delta",
        )

        assert appender.exists()

    def test_exists_method_with_table_as_dataset_type_and_external_path_defined(
        self, spark_schema: str, tmp_path: Path, sample_dataframe: DataFrame
    ):
        table_name = "test_table"
        external_path = str(tmp_path / table_name)
        appender = DeltaTableAppender(
            table=table_name,
            schema=spark_schema,
            dataset_type="table",
            external_path=external_path,
        )
        assert not appender.exists()

        sample_dataframe.write.saveAsTable(
            f"{spark_schema}.{table_name}", mode="append", format="delta", path=external_path
        )

        assert appender.exists()

    def test_exists_method_with_file_as_dataset_type(
        self, tmp_path: Path, sample_dataframe: DataFrame
    ):
        table_name = "test_table"
        appender = DeltaTableAppender(
            table=table_name,
            schema=str(tmp_path),
            dataset_type="file",
        )
        assert not appender.exists()

        sample_dataframe.write.format("delta").save(str(tmp_path / table_name))

        assert appender.exists()

    def test_append_to_file(
        self,
        spark: SparkSession,
        tmp_path: Path,
        sample_dataframe: DataFrame,
    ):
        table_name = "test_table"
        appender = DeltaTableAppender(
            table=table_name,
            schema=str(tmp_path),
            dataset_type="file",
            # These args are only passed to check that they are overwritten by DeltaTableAppender
            # to mode:append and format:delta
            save_args={"mode": "overwrite", "format": "csv"},
        )
        appender.append(sample_dataframe)

        df = spark.read.format("delta").load(str(tmp_path / table_name))
        assert df.count() == sample_dataframe.count()
        assert df.columns == sample_dataframe.columns

    def test_append_to_table(
        self,
        spark: SparkSession,
        spark_schema: str,
        sample_dataframe: DataFrame,
    ):
        table_name = "test_table"
        appender = DeltaTableAppender(
            table=table_name,
            schema=spark_schema,
            dataset_type="table",
            # These args are only passed to check that they are overwritten by DeltaTableAppender
            # to mode:append and format:delta
            save_args={"mode": "overwrite", "format": "csv"},
        )
        appender.append(sample_dataframe)

        df = spark.read.table(f"{spark_schema}.{table_name}")
        assert df.count() == sample_dataframe.count()
        assert df.columns == sample_dataframe.columns

    def test_append_to_table_with_external_path(
        self,
        spark: SparkSession,
        spark_schema: str,
        tmp_path: Path,
        sample_dataframe: DataFrame,
    ):
        table_name = "test_table"
        appender = DeltaTableAppender(
            table=table_name,
            schema=spark_schema,
            dataset_type="table",
            external_path=str(tmp_path / table_name),
            # These args are only passed to check that they are overwritten by DeltaTableAppender
            # to mode:append and format:delta
            save_args={"mode": "overwrite", "format": "csv"},
        )
        appender.append(sample_dataframe)

        df = spark.read.table(f"{spark_schema}.{table_name}")
        assert df.count() == sample_dataframe.count()
        assert df.columns == sample_dataframe.columns

    def test_error_on_exists_method(self):
        """
        Tests that an error is raised if a AnalysisException is raised when checking if a table
        exists and the error message does not contain the string 'is not a Delta table'
        """
        appender = DeltaTableAppender(
            table="test_table", schema="test_schema", dataset_type="table"
        )
        with patch("delta.DeltaTable.forName") as mock_for_name:
            mock_for_name.side_effect = AnalysisException("test")
            with pytest.raises(DeltaTableAppenderError):
                appender.exists()
