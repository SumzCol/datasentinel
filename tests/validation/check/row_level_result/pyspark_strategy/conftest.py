import pytest
from pyspark.sql import SparkSession


def _setup_spark_session():
    return SparkSession.builder.appName("test").master("local[*]").getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def spark():
    spark = _setup_spark_session()
    yield spark
    spark.stop()
