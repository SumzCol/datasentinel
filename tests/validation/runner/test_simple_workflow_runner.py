from unittest.mock import Mock

import pytest

from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.result import DataValidationResult
from dataguard.validation.runner.core import CriticalCheckFailedError
from dataguard.validation.runner.simple_workflow_runner import SimpleWorkflowRunner
from dataguard.validation.status import Status
from dataguard.validation.workflow import ValidationWorkflow


@pytest.mark.unit
@pytest.mark.runners
class TestSimpleWorkflowRunner:
    @pytest.fixture
    def runner(self):
        """Create a SimpleWorkflowRunner instance for testing."""
        return SimpleWorkflowRunner()

    @pytest.fixture
    def mock_validation_workflow(self):
        """Create a mock ValidationWorkflow with all necessary attributes."""
        workflow = Mock(spec=ValidationWorkflow)
        workflow.data_validation = Mock()
        workflow.notifiers_by_event = {}
        workflow.result_stores = []
        return workflow

    @pytest.fixture
    def mock_managers(self):
        """Create mock notifier and result store managers."""
        return (Mock(spec=AbstractNotifierManager), Mock(spec=AbstractResultStoreManager))

    def test_run_successful_validation_workflow(
        self, runner, mock_validation_workflow, mock_managers
    ):
        """Test successful validation workflow execution."""
        notifier_manager, result_store_manager = mock_managers

        # Setup successful validation result
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.name = "test_validation"
        result.status = Status.PASS
        result.failed_checks_by_level.return_value = []

        mock_validation_workflow.data_validation.run.return_value = result

        # Execute - should complete without exceptions
        runner.run(mock_validation_workflow, notifier_manager, result_store_manager)

        # Verify all components were called
        mock_validation_workflow.data_validation.run.assert_called_once()
        notifier_manager.notify_all_by_event.assert_called_once_with(
            notifiers_by_events=mock_validation_workflow.notifiers_by_event, result=result
        )
        result_store_manager.store_all.assert_called_once_with(
            result_stores=mock_validation_workflow.result_stores, result=result
        )

    def test_run_validation_with_warning_failures(
        self, runner, mock_validation_workflow, mock_managers
    ):
        """Test validation workflow with warning-level failures completes successfully."""
        notifier_manager, result_store_manager = mock_managers

        # Setup validation result with warning failures
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.name = "test_validation"
        result.status = Status.FAIL

        warning_check = Mock(spec=CheckResult)
        warning_check.name = "warning_check"
        warning_check.failed_rules = [Mock(rule="range_check", column="age")]

        result.failed_checks_by_level.side_effect = lambda level: (
            [warning_check] if level == CheckLevel.WARNING else []
        )

        mock_validation_workflow.data_validation.run.return_value = result

        # Execute - should complete without exceptions (warnings don't raise)
        runner.run(mock_validation_workflow, notifier_manager, result_store_manager)

        # Verify workflow completed
        mock_validation_workflow.data_validation.run.assert_called_once()
        notifier_manager.notify_all_by_event.assert_called_once()
        result_store_manager.store_all.assert_called_once()

    def test_run_validation_with_critical_failures_raises_exception(
        self, runner, mock_validation_workflow, mock_managers
    ):
        """Test validation workflow with critical failures raises CriticalCheckFailedError."""
        notifier_manager, result_store_manager = mock_managers

        # Setup validation result with critical failures
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.name = "test_validation"
        result.status = Status.FAIL

        critical_check = Mock(spec=CheckResult)
        critical_check.name = "critical_check"
        critical_check.failed_rules = [Mock(rule="data_freshness", column=None)]

        result.failed_checks_by_level.side_effect = lambda level: (
            [critical_check] if level == CheckLevel.CRITICAL else []
        )

        mock_validation_workflow.data_validation.run.return_value = result

        # Execute - should raise CriticalCheckFailedError
        with pytest.raises(CriticalCheckFailedError) as exc_info:
            runner.run(mock_validation_workflow, notifier_manager, result_store_manager)

        # Verify error message contains expected information
        error_message = str(exc_info.value)
        assert "test_table" in error_message
        assert "test_schema" in error_message
        assert "critical_check" in error_message

        # Verify workflow components were still called before exception
        mock_validation_workflow.data_validation.run.assert_called_once()
        notifier_manager.notify_all_by_event.assert_called_once()
        result_store_manager.store_all.assert_called_once()

    def test_run_validation_with_mixed_failure_levels_no_critical(
        self, runner, mock_validation_workflow, mock_managers
    ):
        """Test validation workflow with warning and error failures (no critical)
        completes successfully."""
        notifier_manager, result_store_manager = mock_managers

        # Setup validation result with warning and error failures
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.name = "test_validation"
        result.status = Status.FAIL

        warning_check = Mock(spec=CheckResult)
        warning_check.name = "warning_check"
        warning_check.failed_rules = [Mock(rule="range_check", column="age")]

        error_check = Mock(spec=CheckResult)
        error_check.name = "error_check"
        error_check.failed_rules = [Mock(rule="format_check", column=None)]

        def mock_failed_checks_by_level(level):
            if level == CheckLevel.WARNING:
                return [warning_check]
            elif level == CheckLevel.ERROR:
                return [error_check]
            else:
                return []

        result.failed_checks_by_level.side_effect = mock_failed_checks_by_level
        mock_validation_workflow.data_validation.run.return_value = result

        # Execute - should complete without exceptions (no critical failures)
        runner.run(mock_validation_workflow, notifier_manager, result_store_manager)

        # Verify workflow completed
        mock_validation_workflow.data_validation.run.assert_called_once()
        notifier_manager.notify_all_by_event.assert_called_once()
        result_store_manager.store_all.assert_called_once()
