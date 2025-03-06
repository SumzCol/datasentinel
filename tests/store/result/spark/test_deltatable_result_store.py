import pytest

from dataguard.store.result.core import ResultStoreError
from dataguard.store.result.spark.deltatable_result_store import DeltaTableResultStore


@pytest.mark.unit
class TestDeltaTableResultStoreUnit:
    # @pytest.mark.parametrize(
    #     "dataset_type, schema",
    #     [
    #         ("file", "s3a://test_schema"),
    #         ("table", "catalog.schema"),
    #     ]
    # )
    # def test_initialization_success(self, dataset_type: Literal["file", "table"], schema: str):
    #     store = DeltaTableResultStore(
    #         name="test",
    #         table="test_table",
    #         schema=schema,
    #         dataset_type=dataset_type,
    #         failed_rows_limit=100,
    #         disabled=False,
    #     )
    #
    #     assert store.name == "test"
    #     assert store.disabled == False

    def test_error_on_failed_rows_limit_value(self):
        with pytest.raises(ResultStoreError, match="Failed rows limit must be greater than 0"):
            DeltaTableResultStore(
                name="test",
                table="test_table",
                schema="s3a://test_schema",
                dataset_type="file",
                include_failed_rows=True,
                failed_rows_limit=0,
            )
