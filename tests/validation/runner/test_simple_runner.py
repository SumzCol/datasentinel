from collections.abc import Callable
from unittest.mock import Mock, PropertyMock, patch

import pytest

from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.data_validation import DataValidation
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.runner.core import CriticalCheckFailedError
from dataguard.validation.runner.simple_runner import SimpleRunner
from dataguard.validation.status import Status


@pytest.fixture(scope="function")
def data_validation_result_mock_factory():
    def _create(status: Status, failed_check_level: CheckLevel):
        def failed_checks_by_level_mock(check_level: CheckLevel) -> list[CheckResult]:
            if check_level == failed_check_level:
                rule_metric_mock = Mock(spec=RuleMetric)
                rule_metric_mock.rule = "test_rule"
                rule_metric_mock.column = ["test_column"]

                check_mock = Mock(spec=CheckResult)
                check_mock.name = "test_check"
                type(check_mock).failed_rules = (
                    PropertyMock(return_value=[rule_metric_mock])
                    if status == Status.FAIL
                    else PropertyMock(return_value=[])
                )

                return [check_mock]

            return []

        data_validation_result_mock = Mock(spec=DataValidationResult)
        data_validation_result_mock.name = "test_data_validation"
        data_validation_result_mock.data_asset = "test_data_asset"
        if status == Status.FAIL:
            data_validation_result_mock.failed_checks_by_level.side_effect = (
                failed_checks_by_level_mock
            )
        else:
            data_validation_result_mock.failed_checks_by_level.return_value = []

        return data_validation_result_mock

    return _create


@pytest.fixture(scope="function")
def notifier_manager_mock():
    return Mock(spec=AbstractNotifierManager)


@pytest.fixture(scope="function")
def result_store_manager_mock():
    return Mock(spec=AbstractResultStoreManager)


@pytest.fixture(scope="function")
def data_validation_mock():
    return Mock(spec=DataValidation)


@pytest.mark.unit
class TestSimpleRunnerUnit:
    @patch.object(SimpleRunner, "_run")
    def test_error_on_failed_critical_checks(
        self,
        run_mock: Mock,
        data_validation_result_mock_factory: Callable,
        notifier_manager_mock: Mock,
        result_store_manager_mock: Mock,
        data_validation_mock: Mock,
    ):
        runner = SimpleRunner()
        result_mock = data_validation_result_mock_factory(Status.FAIL, CheckLevel.CRITICAL)
        run_mock.return_value = result_mock

        with pytest.raises(CriticalCheckFailedError):
            runner.run(
                data_validation=data_validation_mock,
                notifier_manager=notifier_manager_mock,
                result_store_manager=result_store_manager_mock,
            )
