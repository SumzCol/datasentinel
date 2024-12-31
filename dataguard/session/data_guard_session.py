import logging
import threading
from typing import Dict

from ulid import ULID

from dataguard.session.core import (
    SessionNotSpecifiedError,
    SessionAlreadyExistsError,
)
from dataguard.store.audit.manager import AuditStoreManager
from dataguard.store.result.manager import ResultStoreManager
from dataguard.notification.notifier.manager import NotifierManager
from dataguard.validation.datasource.core import AbstractDatasource
from dataguard.validation.node.validation_node import ValidationNode
from dataguard.validation.runner.core import AbstractRunner
from dataguard.validation.runner.simple_runner import SimpleRunner


class DataGuardSession:
    _active_sessions = {}
    _lock = threading.Lock()

    def __init__(self, name):
        if name in DataGuardSession._active_sessions:
            raise SessionAlreadyExistsError(f"A session with name '{name}' already exists")
        self.name = name
        self._notifier_manager = NotifierManager()
        self._result_store_manager = ResultStoreManager()
        self._audit_store_manager = AuditStoreManager()
        self._validation_nodes: Dict[str, ValidationNode] = {}
        DataGuardSession._active_sessions[name] = self

    @property
    def logger(self):
        return logging.getLogger(__name__)

    @classmethod
    def get_or_create(cls, name: str | None = None) -> "DataGuardSession":
        if name is None and len(cls._active_sessions) == 0:
            with cls._lock:
                return cls(str(ULID()))

        if name is None and len(cls._active_sessions) == 1:
            return next(iter(cls._active_sessions.values()))

        if name is None and len(cls._active_sessions) > 1:
            raise SessionNotSpecifiedError(
                "No name specified and there are multiple active sessions. "
                "Specify a name."
            )

        if name in cls._active_sessions:
            return cls._active_sessions[name]
        else:
            with cls._lock:
                return cls(name)

    @property
    def notifier_manager(self) -> NotifierManager:
        return self._notifier_manager

    @property
    def result_store_manager(self) -> ResultStoreManager:
        return self._result_store_manager

    @property
    def audit_store_manager(self) -> AuditStoreManager:
        return self._audit_store_manager

    def run_validation_node(
        self,
        validation_node: ValidationNode,
        dataset: AbstractDatasource | None = None,
        runner: AbstractRunner | None = None
    ):
        runner = runner or SimpleRunner()
        runner.run(
            validation_node=validation_node,
            datasource=dataset,
            notifier_manager=self._notifier_manager,
            result_store_manager=self._result_store_manager
        )

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"