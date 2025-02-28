import logging
import threading
from typing import ClassVar

from ulid import ULID

from dataguard.notification.notifier.core import AbstractNotifierManager
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.session.core import (
    SessionAlreadyExistsError,
    SessionNotSpecifiedError,
)
from dataguard.store.audit.core import AbstractAuditStoreManager
from dataguard.store.audit.manager import AuditStoreManager
from dataguard.store.result.core import AbstractResultStoreManager
from dataguard.store.result.manager import ResultStoreManager
from dataguard.validation.data_asset.core import AbstractDataAsset
from dataguard.validation.data_validation import DataValidation
from dataguard.validation.runner.core import AbstractRunner
from dataguard.validation.runner.simple_runner import SimpleRunner


class DataGuardSession:
    """Entry point to access all the functionalities of DataGuard."""

    _active_sessions: ClassVar[dict[str, "DataGuardSession"]] = {}
    _lock = threading.Lock()

    def __init__(
        self,
        name: str,
        notifier_manager: AbstractNotifierManager | None = None,
        result_store_manager: AbstractResultStoreManager | None = None,
        audit_store_manager: AbstractAuditStoreManager | None = None,
    ):
        if name in DataGuardSession._active_sessions:
            raise SessionAlreadyExistsError(f"A session with name '{name}' already exists")
        self.name = name
        self._notifier_manager = notifier_manager or NotifierManager()
        self._result_store_manager = result_store_manager or ResultStoreManager()
        self._audit_store_manager = audit_store_manager or AuditStoreManager()
        DataGuardSession._active_sessions[name] = self

    @property
    def _logger(self) -> logging.Logger:  # pragma: no cover
        return logging.getLogger(__name__)

    @classmethod
    def get_or_create(cls, name: str | None = None, **kwargs) -> "DataGuardSession":
        """Get or create a new DataGuard session

        Args:
            name: Name of session to be created or retrieved if a session exists with the
                same name
            **kwargs: Additional arguments passed to the DataGuardSession constructor.

        Returns:
            The session created or retrieved
        """
        if name is None and len(cls._active_sessions) == 0:
            with cls._lock:
                return cls(str(ULID()), **kwargs)

        if name is None and len(cls._active_sessions) == 1:
            return next(iter(cls._active_sessions.values()))

        if name is None and len(cls._active_sessions) > 1:
            raise SessionNotSpecifiedError(
                "No name specified and there are multiple active sessions. Specify a name."
            )

        if name in cls._active_sessions:
            return cls._active_sessions[name]
        else:
            with cls._lock:
                return cls(name, **kwargs)

    @property
    def notifier_manager(self) -> AbstractNotifierManager:
        """Return the notifier manager"""
        return self._notifier_manager

    @property
    def result_store_manager(self) -> AbstractResultStoreManager:
        """Return the result store manager"""
        return self._result_store_manager

    @property
    def audit_store_manager(self) -> AbstractAuditStoreManager:
        """Return the audit store manager"""
        return self._audit_store_manager

    def run_data_validation(
        self,
        data_validation: DataValidation,
        data_asset: AbstractDataAsset | None = None,
        runner: AbstractRunner | None = None,
    ) -> None:
        """Runs a data validation.

        Args:
            data_validation: Data validation to run
            data_asset: Data asset to validate or replace the one in the data validation
                if one is defined.
            runner: Runner to run the data validation
        """
        runner = runner or SimpleRunner()
        runner.run(
            data_validation=data_validation,
            data_asset=data_asset,
            notifier_manager=self._notifier_manager,
            result_store_manager=self._result_store_manager,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}(name={self.name})"
