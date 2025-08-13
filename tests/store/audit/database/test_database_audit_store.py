import pytest

from dataguard.store.audit.database import DatabaseAuditStore


@pytest.mark.unit
@pytest.mark.audit_store
class TestDatabaseAuditStoreUnit:
    def test_name_property(self):
        expected_name = "test"
        store = DatabaseAuditStore(
            name=expected_name,
            disabled=False,
            table="test_table",
            schema="test_schema",
            credentials={
                "connection_string": "sqlite:///test.db",
            },
        )

        assert store.name == expected_name

    def test_disabled_property(self):
        expected_disabled = True
        store = DatabaseAuditStore(
            name="test",
            disabled=expected_disabled,
            table="test_table",
            schema="test_schema",
            credentials={
                "connection_string": "sqlite:///test.db",
            },
        )

        assert store.disabled == expected_disabled
