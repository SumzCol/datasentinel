from typing import Union, Any

from dataguard.session.core import ValidationSessionException
from dataguard.session.manager import NotifierManager, MetricStoreManager
from dataguard.session.session_configuration import SessionConfiguration


class ValidationSession:
    _active_sessions = {}

    def __init__(self, name, **configuration):
        self.name = name
        self._configuration = SessionConfiguration(**configuration)
        self._notifier_manager = NotifierManager()
        self._metric_store_manager = MetricStoreManager()
        ValidationSession._active_sessions[name] = self

    @property
    def conf(self):
        return self._configuration

    @classmethod
    def get_active_session(cls) -> Union["ValidationSession", None]:
        if len(cls._active_sessions) == 0:
            return None
        elif len(cls._active_sessions) == 1:
            return next(iter(cls._active_sessions.values()))
        else:
            raise ValidationSessionException(
                "Multiple active sessions exist. Use get_or_create to specify a name."
            )

    @classmethod
    def get_or_create(cls, name: str | None = None, **configurations) -> Union["ValidationSession", None]:
        if name is None and len(cls._active_sessions) == 1:
            return next(iter(cls._active_sessions.values()))

        if name is None and len(cls._active_sessions) > 1:
            raise ValidationSessionException(
                "No name specified and there is multiple active sessions. "
                "Specify a name to create a new session."
            )

        if name is None:
            raise ValidationSessionException(
                "No name specified and there is no active session."
            )

        if name in cls._active_sessions:
            return cls._active_sessions[name]
        else:
            return cls(name, **configurations)

    def register_notifier(self, name: str, notifier) -> None:
        self._notifier_manager.register_notifier(name, notifier)

    def register_metric_store(self, name: str, metric_store) -> None:
        self._metric_store_manager.register_metric_store(name, metric_store)

    def register_validation_suite(self, validation_suite) -> None:
        pass

    def run_validation_suite(self, validation_suite: str, data: Any, runner) -> None:
        pass

    def stop(self):
        if self.name in ValidationSession._active_sessions:
            del ValidationSession._active_sessions[self.name]
        else:
            raise ValidationSessionException(f"'{self.name}' session does not exist.")

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, configuration={self._configuration})"