import pytest

from dataguard.store.result.spark.deltatable_result_store import DeltaTableResultStore


@pytest.mark.unit
class TestDeltaTableResultStoreUnit:
    def test_name_property(self):
        expected_name = "test"
        store = DeltaTableResultStore(
            name=expected_name,
            table="test_table",
            schema="test_schema",
            dataset_type="file",
        )

        assert store.name == expected_name

    def test_disabled_property(self):
        expected_disabled = True
        store = DeltaTableResultStore(
            name="test",
            disabled=expected_disabled,
            table="test_table",
            schema="test_schema",
            dataset_type="file",
        )

        assert store.disabled == expected_disabled
