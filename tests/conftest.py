import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def _setup_spark_session(include_delta: bool):
    builder = SparkSession.builder.appName("test").master("local[*]")

    if not include_delta:
        return builder.getOrCreate()

    return configure_spark_with_delta_pip(
        builder.config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    ).getOrCreate()


@pytest.fixture(scope="session")
def spark():
    spark = _setup_spark_session(include_delta=False)
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_delta():
    spark = _setup_spark_session(include_delta=True)
    yield spark
    spark.stop()
