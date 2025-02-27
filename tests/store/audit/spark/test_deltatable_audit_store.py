import pytest

from dataguard.store.audit.spark.deltatable_audit_store import DeltaTableAuditStore


@pytest.mark.unit
class TestDeltaTableAuditStoreUnit:
    def test_name_property(self):
        expected_name = "test"
        store = DeltaTableAuditStore(
            name=expected_name,
            table="test_table",
            schema="test_schema",
            dataset_type="file",
        )

        assert store.name == expected_name

    def test_disabled_property(self):
        expected_disabled = True
        store = DeltaTableAuditStore(
            name="test",
            disabled=expected_disabled,
            table="test_table",
            schema="test_schema",
            dataset_type="file",
        )

        assert store.disabled == expected_disabled
