from unittest.mock import Mock

import pytest

from dataguard.store.audit.core import (
    AbstractAuditStore,
    AuditStoreAlreadyExistsError,
    AuditStoreError,
    AuditStoreNotFoundError,
)
from dataguard.store.audit.manager import AuditStoreManager
from dataguard.store.audit.row import BaseAuditRow


@pytest.fixture
def audit_store():
    def _audit_store(name: str, disabled: bool = False):
        audit_store_mock = Mock(spec=AbstractAuditStore)
        audit_store_mock.name = name
        audit_store_mock.disabled = disabled
        return audit_store_mock

    return _audit_store


@pytest.mark.unit
class TestAuditStoreManagerUnit:
    def test_count(self, audit_store):
        manager = AuditStoreManager()
        manager.register(
            audit_store=audit_store(name="test", disabled=False),
        )
        manager.register(
            audit_store=audit_store(name="test2", disabled=True),
        )
        expected_enabled_only_count = 1
        expected_all_count = 2

        assert manager.count(enabled_only=False) == expected_all_count
        assert manager.count(enabled_only=True) == expected_enabled_only_count

    def test_get(self, audit_store):
        manager = AuditStoreManager()
        audit_store_name = "test"
        manager.register(
            audit_store=audit_store(name=audit_store_name),
        )

        assert manager.get(audit_store_name)
        with pytest.raises(AuditStoreNotFoundError):
            manager.get("not_existing_name")

    def test_register(self, audit_store):
        result_store_name = "test"
        manager = AuditStoreManager()
        manager.register(
            audit_store=audit_store(name=result_store_name),
        )
        # Test that replace=True works, as it shouldn't raise any errors
        manager.register(
            audit_store=audit_store(name=result_store_name),
            replace=True,
        )

        assert manager.get(result_store_name)
        with pytest.raises(AuditStoreAlreadyExistsError):
            manager.register(
                audit_store=audit_store(name=result_store_name),
                replace=False,
            )

    def test_remove(self, audit_store):
        manager = AuditStoreManager()
        result_store_name = "test"
        manager.register(
            audit_store=audit_store(name=result_store_name),
        )
        manager.remove(result_store_name)

        assert not manager.exists(result_store_name)
        with pytest.raises(AuditStoreNotFoundError):
            manager.remove("not_existing_name")

    def test_exists(self, audit_store):
        manager = AuditStoreManager()
        result_store_name = "test"
        manager.register(
            audit_store=audit_store(name=result_store_name),
        )

        assert manager.exists(result_store_name)
        assert not manager.exists("not_existing_name")

    def test_append(self, audit_store):
        manager = AuditStoreManager()
        enabled_audit_store = audit_store(name="enabled_store", disabled=False)
        disabled_audit_store = audit_store(name="disabled_store", disabled=True)
        manager.register(audit_store=enabled_audit_store)
        manager.register(audit_store=disabled_audit_store)

        manager.append(
            audit_store="enabled_store",
            row=Mock(spec=BaseAuditRow),
        )
        manager.append(
            audit_store="disabled_store",
            row=Mock(spec=BaseAuditRow),
        )

        enabled_audit_store.append.assert_called_once()
        disabled_audit_store.append.assert_not_called()

    def test_append_with_an_exception_on_audit_store(self, audit_store):
        manager = AuditStoreManager()
        audit_store = audit_store(name="error_store", disabled=False)
        audit_store.append.side_effect = AuditStoreError("Mock error")

        manager.register(audit_store=audit_store)

        manager.append(
            audit_store="error_store",
            row=Mock(spec=BaseAuditRow),
        )

        audit_store.append.assert_called_once()

    def test_append_to_all_stores(self, audit_store):
        manager = AuditStoreManager()
        enabled_audit_store = audit_store(name="enabled_store", disabled=False)
        disabled_audit_store = audit_store(name="disabled_store", disabled=True)
        manager.register(audit_store=enabled_audit_store)
        manager.register(audit_store=disabled_audit_store)

        manager.append_to_all_stores(
            row=Mock(spec=BaseAuditRow),
        )

        enabled_audit_store.append.assert_called_once()
        disabled_audit_store.append.assert_not_called()

    def test_append_to_all_stores_with_an_exception_on_audit_store(self, audit_store):
        manager = AuditStoreManager()
        audit_store = audit_store(name="error_store", disabled=False)
        audit_store.append.side_effect = AuditStoreError("Mock error")

        manager.register(audit_store=audit_store)

        manager.append_to_all_stores(
            row=Mock(spec=BaseAuditRow),
        )

        audit_store.append.assert_called_once()
