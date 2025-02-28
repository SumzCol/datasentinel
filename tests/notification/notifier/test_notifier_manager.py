from unittest.mock import Mock, PropertyMock

import pytest

from dataguard.notification.notifier.core import (
    AbstractNotifier,
    NotifierAlreadyExistsError,
    NotifierError,
    NotifierNotFoundError,
)
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.validation.core import NotifyOnEvent
from dataguard.validation.result import DataValidationResult
from dataguard.validation.status import Status


@pytest.fixture
def notifier_mock():
    def _notifier(name: str, disabled: bool = False):
        notifier_mock = Mock(spec=AbstractNotifier)
        notifier_mock.name = name
        notifier_mock.disabled = disabled
        return notifier_mock

    return _notifier


@pytest.mark.unit
class TestNotifierManagerUnit:
    def test_count(self, notifier_mock):
        manager = NotifierManager()
        manager.register(
            notifier=notifier_mock(name="test", disabled=False),
        )
        manager.register(
            notifier=notifier_mock(name="test2", disabled=True),
        )
        expected_enabled_only_count = 1
        expected_all_count = 2

        assert manager.count(enabled_only=False) == expected_all_count
        assert manager.count(enabled_only=True) == expected_enabled_only_count

    def test_get(self, notifier_mock):
        manager = NotifierManager()
        notifier_name = "test"
        manager.register(
            notifier=notifier_mock(name=notifier_name),
        )

        assert manager.get(notifier_name)
        with pytest.raises(NotifierNotFoundError):
            manager.get("not_existing_name")

    def test_register(self, notifier_mock):
        notifier_name = "test"
        manager = NotifierManager()
        manager.register(
            notifier=notifier_mock(name=notifier_name),
        )
        # Test that replace=True works, as it shouldn't raise any errors
        manager.register(
            notifier=notifier_mock(name=notifier_name),
            replace=True,
        )

        assert manager.get(notifier_name)
        with pytest.raises(NotifierAlreadyExistsError):
            manager.register(
                notifier=notifier_mock(name=notifier_name),
                replace=False,
            )

    def test_remove(self, notifier_mock):
        manager = NotifierManager()
        notifier_name = "test"
        manager.register(
            notifier=notifier_mock(name=notifier_name),
        )
        manager.remove(notifier_name)

        assert not manager.exists(notifier_name)
        with pytest.raises(NotifierNotFoundError):
            manager.remove("not_existing_name")

    def test_exists(self, notifier_mock):
        manager = NotifierManager()
        notifier_name = "test"
        manager.register(
            notifier=notifier_mock(name=notifier_name),
        )

        assert manager.exists(notifier_name)
        assert not manager.exists("not_existing_name")

    @pytest.mark.parametrize(
        "data_validation_status",
        [
            Status.PASS,
            Status.FAIL,
        ],
    )
    def test_notify_all_by_event_based_on_data_validation_result_status(
        self, data_validation_status, notifier_mock
    ):
        manager = NotifierManager()
        on_pass_notifier = notifier_mock(name="on_pass_notifier", disabled=False)
        on_fail_notifier = notifier_mock(name="on_fail_notifier", disabled=False)
        on_all_notifier = notifier_mock(name="on_all_notifier", disabled=False)
        manager.register(notifier=on_pass_notifier)
        manager.register(notifier=on_fail_notifier)
        manager.register(notifier=on_all_notifier)

        data_validation_result = Mock(spec=DataValidationResult)
        type(data_validation_result).status = PropertyMock(return_value=data_validation_status)

        manager.notify_all_by_event(
            notifiers_by_events={
                NotifyOnEvent.PASS: [on_pass_notifier.name],
                NotifyOnEvent.FAIL: [on_fail_notifier.name],
                NotifyOnEvent.ALL: [on_all_notifier.name],
            },
            result=data_validation_result,
        )

        if data_validation_status == Status.PASS:
            on_pass_notifier.notify.assert_called_once()
            on_all_notifier.notify.assert_called_once()
            on_fail_notifier.notify.assert_not_called()
        else:
            on_pass_notifier.notify.assert_not_called()
            on_all_notifier.notify.assert_called_once()
            on_fail_notifier.notify.assert_called_once()

    @pytest.mark.parametrize(
        "data_validation_status",
        [
            Status.PASS,
            Status.FAIL,
        ],
    )
    def test_notify_all_by_event_with_disabled_notifier_based_on_data_validation_result_status(
        self, notifier_mock, data_validation_status
    ):
        manager = NotifierManager()
        disabled_notifier = notifier_mock(name="on_pass_notifier", disabled=True)
        manager.register(notifier=disabled_notifier)

        data_validation_result = Mock(spec=DataValidationResult)
        type(data_validation_result).status = PropertyMock(return_value=data_validation_status)

        notify_on_event = (
            NotifyOnEvent.PASS if data_validation_status == Status.PASS else NotifyOnEvent.FAIL
        )
        manager.notify_all_by_event(
            notifiers_by_events={
                notify_on_event: [disabled_notifier.name],
            },
            result=data_validation_result,
        )

        disabled_notifier.notify.assert_not_called()

    def test_notify_all_by_event_with_exception_on_notifier(self, notifier_mock):
        manager = NotifierManager()
        notifier_with_exception = notifier_mock(name="notifier_with_exception", disabled=False)
        notifier_with_exception.notify.side_effect = NotifierError("Notifier error")
        manager.register(notifier=notifier_with_exception)

        data_validation_result = Mock(spec=DataValidationResult)
        type(data_validation_result).status = PropertyMock(return_value=Status.FAIL)

        manager.notify_all_by_event(
            notifiers_by_events={
                NotifyOnEvent.FAIL: [notifier_with_exception.name],
            },
            result=data_validation_result,
        )

        notifier_with_exception.notify.assert_called_once()
