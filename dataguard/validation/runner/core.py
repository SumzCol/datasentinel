import logging
from abc import ABC, abstractmethod

from dataguard.store.result.manager import ResultStoreManager
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.validation.datasource.core import AbstractDatasource
from dataguard.validation.node.validation_node import ValidationNode


class RunnerError(Exception):
    pass


class NoDatasetDefinedError(RunnerError):
    pass


class NoChecksDefinedError(RunnerError):
    pass


class ErrorLevelCheckFailedError(RunnerError):
    pass


class AbstractRunner(ABC):
    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @abstractmethod
    def run(
        self,
        validation_node: ValidationNode,
        datasource: AbstractDatasource | None,
        metric_store_manager: ResultStoreManager,
        notifier_manager: NotifierManager
    ) -> None:
        """Run a validation node"""