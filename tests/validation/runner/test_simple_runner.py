from unittest.mock import Mock, PropertyMock, patch

import pytest

from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.validation.check.result import CheckResult
from dataguard.validation.data_validation import DataValidation
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.runner.core import CriticalCheckFailedError
from dataguard.validation.runner.simple_runner import SimpleRunner


@pytest.mark.unit
class TestSimpleRunnerUnit:
    @patch.object(SimpleRunner, "_run")
    @patch.object(SimpleRunner, "_log_status")
    @patch.object(SimpleRunner, "_raise_exc_on_failed_critical_checks")
    def test_notify_and_result_store_on_run(self, raise_exc_mock, log_mock, run_mock):
        notifier_manager_mock = Mock(spec=AbstractNotifierManager)
        result_store_manager_mock = Mock(spec=AbstractResultStoreManager)
        data_validation_mock = Mock(spec=DataValidation)
        data_validation_result_mock = Mock(spec=DataValidationResult)
        run_mock.return_value = data_validation_result_mock

        runner = SimpleRunner()
        runner.run(
            data_validation=data_validation_mock,
            notifier_manager=notifier_manager_mock,
            result_store_manager=result_store_manager_mock,
        )

        notifier_manager_mock.notify_all_by_event.assert_called_once_with(
            notifiers_by_events=data_validation_mock.notifiers_by_event,
            result=data_validation_result_mock,
        )
        result_store_manager_mock.store_all.assert_called_once_with(
            result_stores=data_validation_mock.result_stores,
            result=data_validation_result_mock,
        )

    @patch.object(SimpleRunner, "_run")
    @patch.object(SimpleRunner, "_log_status")
    def test_raise_exception_on_failed_critical_checks(self, log_mock, run_mock):
        runner = SimpleRunner()

        rule_metric_mock = Mock(spec=RuleMetric)
        rule_metric_mock.rule = "test_rule"
        rule_metric_mock.column = ["test_column"]

        check_mock = Mock(spec=CheckResult)
        check_mock.name = "test_check"
        type(check_mock).failed_rules = PropertyMock(return_value=[rule_metric_mock])

        data_validation_result_mock = Mock(spec=DataValidationResult)
        data_validation_result_mock.name = "test_data_validation"
        data_validation_result_mock.data_asset = "test_data_asset"
        data_validation_result_mock.failed_checks_by_level.return_value = [check_mock]

        run_mock.return_value = data_validation_result_mock

        with pytest.raises(CriticalCheckFailedError):
            runner.run(
                data_validation=Mock(spec=DataValidation),
                notifier_manager=Mock(spec=AbstractNotifierManager),
                result_store_manager=Mock(spec=AbstractResultStoreManager),
            )
        log_mock.assert_called_once_with(
            result=data_validation_result_mock,
        )
