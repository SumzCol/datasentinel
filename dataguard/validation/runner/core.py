import logging
from abc import ABC, abstractmethod
from typing import List

from dataguard.core import DataGuardError
from dataguard.store.result.manager import ResultStoreManager
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.data_asset.core import AbstractDataAsset
from dataguard.validation.result import DataValidationResult
from dataguard.validation.data_validation import DataValidation
from dataguard.validation.status import Status


class RunnerError(DataGuardError):
    pass


class NoDatasetDefinedError(RunnerError):
    pass


class NoChecksDefinedError(RunnerError):
    pass


class CriticalCheckFailedError(RunnerError):
    pass


class AbstractRunner(ABC):
    """Base class for all runner implementations."""
    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    def run(
        self,
        data_validation: DataValidation,
        data_asset: AbstractDataAsset | None,
        result_store_manager: ResultStoreManager,
        notifier_manager: NotifierManager
    ) -> None:
        """Run a validation node"""
        if data_asset is None and data_validation.data_asset is None:
            raise NoDatasetDefinedError(
                f"No dataset to be validated was passed or defined inside the "
                f"validation node '{data_validation.name}'"
            )
        data_asset = data_asset or data_validation.data_asset

        if not data_validation.has_checks:
            raise NoChecksDefinedError(
                f"No checks were defined in validation node '{data_validation.name}'"
            )

        validation_node_result = self._run(
            data_validation=data_validation,
            data_asset=data_asset
        )

        self._log_status(result=validation_node_result)

        notifier_manager.notify_all_by_event(
            notifiers_by_events=data_validation.notifiers_by_event,
            result=validation_node_result
        )
        result_store_manager.store_all(
            result_stores=data_validation.result_stores,
            result=validation_node_result
        )

        _raise_exc_on_failed_critical_checks(result=validation_node_result)

    @abstractmethod
    def _run(
        self,
        data_validation: DataValidation,
        data_asset: AbstractDataAsset
    ) -> DataValidationResult:
        pass

    def _log_status(
        self,
        result: DataValidationResult
    ):
        data_asset_info = (
            f"Data asset '{result.data_asset}' in schema '{result.data_asset_schema}'"
            if result.data_asset_schema is not None
            else f"Data asset '{result.data_asset}'"
        )

        if result.status == Status.PASS:
            self._logger.info(
                f"{data_asset_info} passed all checks on "
                f"validation node '{result.name}'"
            )
            return

        _logger_methods_map = {
            CheckLevel.WARNING: self._logger.warning,
            CheckLevel.ERROR: self._logger.error,
            CheckLevel.CRITICAL: self._logger.critical,
        }

        for level, method in _logger_methods_map.items():
            failed_checks = result.failed_checks_by_level(level)
            if not failed_checks:
                continue

            summary = _failed_checks_summary(result.failed_checks_by_level(level))
            method(
                f"{data_asset_info} failed checks: {summary} on validation node '{result.name}'"
            )


def _failed_checks_summary(
    failed_checks: List[CheckResult]
) -> str:
    failed_checks_str = []
    for failed_check in failed_checks:
        failed_rules_str = ", ".join(
            [
                f"{rule_metric.rule}[column: {rule_metric.column}]"
                if rule_metric.column is not None
                else rule_metric.rule
                for rule_metric in failed_check.failed_rules
            ]
        )
        failed_checks_str.append(f"{failed_check.name}({failed_rules_str})")

    return ", ".join(failed_checks_str)


def _raise_exc_on_failed_critical_checks(
    result: DataValidationResult
):
    critical_failed_checks = result.failed_checks_by_level(CheckLevel.CRITICAL)
    if critical_failed_checks:
        summary = _failed_checks_summary(critical_failed_checks)
        data_asset_info = (
            f"Data asset '{result.data_asset}' in schema '{result.data_asset_schema}'"
            if result.data_asset_schema is not None
            else f"Data asset '{result.data_asset}'"
        )
        raise CriticalCheckFailedError(
            f"{data_asset_info} failed checks: {summary} on validation node '{result.name}'"
        )
