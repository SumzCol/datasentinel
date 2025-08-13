from unittest.mock import Mock

import pytest

from dataguard.store.result.core import (
    AbstractResultStore,
    ResultStoreAlreadyExistsError,
    ResultStoreError,
    ResultStoreNotFoundError,
)
from dataguard.store.result.manager import ResultStoreManager
from dataguard.validation.result import DataValidationResult


@pytest.fixture
def result_store():
    def _result_store(name: str, disabled: bool = False):
        result_store_mock = Mock(spec=AbstractResultStore)
        result_store_mock.name = name
        result_store_mock.disabled = disabled
        return result_store_mock

    return _result_store


@pytest.mark.unit
@pytest.mark.result_store
class TestResultStoreManagerUnit:
    def test_count(self, result_store):
        manager = ResultStoreManager()
        manager.register(
            result_store=result_store(name="test", disabled=False),
        )
        manager.register(
            result_store=result_store(name="test2", disabled=True),
        )
        expected_enabled_only_count = 1
        expected_all_count = 2

        assert manager.count(enabled_only=False) == expected_all_count
        assert manager.count(enabled_only=True) == expected_enabled_only_count

    def test_get(self, result_store):
        manager = ResultStoreManager()
        expected_name = "test"
        manager.register(
            result_store=result_store(name=expected_name),
        )

        assert manager.get(expected_name)
        with pytest.raises(ResultStoreNotFoundError):
            manager.get("not_existing_name")

    def test_register(self, result_store):
        result_store_name = "test"
        manager = ResultStoreManager()
        manager.register(
            result_store=result_store(name=result_store_name),
        )
        # Test that replace=True works, as it shouldn't raise any errors
        manager.register(
            result_store=result_store(name=result_store_name),
            replace=True,
        )

        assert manager.get(result_store_name)
        with pytest.raises(ResultStoreAlreadyExistsError):
            manager.register(
                result_store=result_store(name=result_store_name),
                replace=False,
            )

    def test_remove(self, result_store):
        manager = ResultStoreManager()
        result_store_name = "test"
        manager.register(
            result_store=result_store(name=result_store_name),
        )
        manager.remove(result_store_name)

        assert not manager.exists(result_store_name)
        with pytest.raises(ResultStoreNotFoundError):
            manager.remove("not_existing_name")

    def test_exists(self, result_store):
        manager = ResultStoreManager()
        result_store_name = "test"
        manager.register(
            result_store=result_store(name=result_store_name),
        )

        assert manager.exists(result_store_name)
        assert not manager.exists("not_existing_name")

    def test_store_all(self, result_store):
        manager = ResultStoreManager()
        enabled_result_store = result_store(name="enabled_store", disabled=False)
        disabled_result_store = result_store(name="disabled_store", disabled=True)
        manager.register(result_store=enabled_result_store)
        manager.register(result_store=disabled_result_store)

        manager.store_all(
            result_stores=["enabled_store", "disabled_store"],
            result=Mock(spec=DataValidationResult),
        )

        enabled_result_store.store.assert_called_once()
        disabled_result_store.store.assert_not_called()

    def test_store_all_with_an_exception_on_result_store(self, result_store):
        manager = ResultStoreManager()
        result_store = result_store(name="error_store", disabled=False)
        result_store.store.side_effect = ResultStoreError("Mock error")

        manager.register(result_store=result_store)

        manager.store_all(
            result_stores=["error_store"],
            result=Mock(spec=DataValidationResult),
        )

        result_store.store.assert_called_once()
