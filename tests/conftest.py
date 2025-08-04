from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import pytest


def _setup_spark_session():
    return configure_spark_with_delta_pip(
        SparkSession.builder.appName("test")
        .master("local[*]")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    ).getOrCreate()


@pytest.fixture(scope="session")
def spark():
    spark = _setup_spark_session()
    yield spark
    spark.stop()
