from abc import ABC, abstractmethod

from dataguard.core import DataGuardError
from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.validation.workflow import ValidationWorkflow


class RunnerError(DataGuardError):
    """Base class for runner errors."""


class NoDatasetDefinedError(RunnerError):
    pass


class CriticalCheckFailedError(RunnerError):
    pass


class AbstractWorkflowRunner(ABC):
    """Base class for all validation workflow runner implementations."""

    @abstractmethod
    def run(
        self,
        validation_workflow: ValidationWorkflow,
        notifier_manager: AbstractNotifierManager,
        result_store_manager: AbstractResultStoreManager,
    ) -> None:
        """Run a validation workflow."""
