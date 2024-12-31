import logging
from datetime import datetime

from ulid import ULID

from dataguard.store.result.manager import ResultStoreManager
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.datasource.core import AbstractDatasource
from dataguard.validation.node.result import ValidationNodeResult
from dataguard.validation.node.validation_node import ValidationNode
from dataguard.validation.runner.core import (
    AbstractRunner,
    NoDatasetDefinedError,
    NoChecksDefinedError,
    ErrorLevelCheckFailedError,
)
from dataguard.validation.status import Status


class SimpleRunner(AbstractRunner):
    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    def run(
        self,
        validation_node: ValidationNode,
        datasource: AbstractDatasource | None,
        result_store_manager: ResultStoreManager,
        notifier_manager: NotifierManager
    ) -> None:
        if datasource is None and validation_node.datasource is None:
            raise NoDatasetDefinedError(
                f"No dataset to be validated was passed or defined inside the "
                f"validation node '{validation_node.name}'"
            )
        datasource = datasource or validation_node.datasource

        if not validation_node.has_checks():
            raise NoChecksDefinedError(
                f"No checks were defined in check list '{validation_node.check_list.name}'"
            )

        start_time = datetime.now()
        data = datasource.load()
        check_results = [
            check.evaluate(data)
            for check in validation_node.check_list
        ]
        end_time = datetime.now()

        validation_node_result = ValidationNodeResult(
            run_id=ULID(),
            name=validation_node.name,
            data_asset=datasource.data_asset,
            data_asset_schema=datasource.data_asset_schema,
            start_time=start_time,
            end_time=end_time,
            check_results=check_results,
            metadata=validation_node.metadata,
        )

        self._log_status(result=validation_node_result)

        notifier_manager.notify_all_by_event(
            notifiers_by_events=validation_node.notifiers_by_events,
            result=validation_node_result
        )
        result_store_manager.store_all(
            result_stores=validation_node.result_stores,
            result=validation_node_result
        )

        self._raise_exc_on_failed_critical_rule(
            result=validation_node_result
        )

    def _log_status(
        self,
        result: ValidationNodeResult
    ):
        if result.status == Status.PASS:
            self._logger.info(
                f"Data asset '{result.data_asset}' passed all checks on "
                f"validation node '{result.name}'"
            )
            return

        failed_checks_str = ",".join(result.get_failed_checks_name())
        self._logger.warning(
            f"Data asset '{result.data_asset}' failed checks "
            f"({failed_checks_str}) on validation node '{result.name}'"
        )

    @staticmethod
    def _raise_exc_on_failed_critical_rule(
        result: ValidationNodeResult
    ):
        if result.error_level_check_failed:
            failed_checks_str = ",".join(
                result.get_failed_checks_name_by_level(
                    level=CheckLevel.ERROR
                )
            )
            raise ErrorLevelCheckFailedError(
                f"Error level checks ({failed_checks_str}) failed on validation "
                f"node '{result.name}' and "
                f"data asset '{result.data_asset}'"
            )
