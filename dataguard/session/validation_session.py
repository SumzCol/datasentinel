import logging
import threading
from typing import Union, Any, Dict

from dataguard.session.core import ValidationSessionException
from dataguard.session.manager import NotifierManager, MetricStoreManager
from dataguard.session.session_configuration import SessionConfiguration
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.suite.result import ValidationSuiteResult
from dataguard.validation.status import Status
from dataguard.validation.suite.validation_suite import ValidationSuite


class ValidationSession:
    _active_sessions = {}
    _lock = threading.Lock()

    def __init__(self, name, **configuration):
        self.name = name
        self._configuration = SessionConfiguration(**configuration)
        self._notifier_manager = NotifierManager()
        self._metric_store_manager = MetricStoreManager()
        self._validation_suites: Dict[str, ValidationSuite] = {}
        ValidationSession._active_sessions[name] = self

    @property
    def conf(self):
        return self._configuration

    @property
    def logger(self):
        return logging.getLogger(__name__)

    @classmethod
    def get_or_create(cls, name: str | None = None, **configurations) -> Union["ValidationSession", None]:
        with cls._lock:
            if name is None and len(cls._active_sessions) == 0:
                raise ValidationSessionException(
                    "No name specified and there are no active sessions."
                )

            if name is None and len(cls._active_sessions) == 1:
                return next(iter(cls._active_sessions.values()))

            if name is None and len(cls._active_sessions) > 1:
                raise ValidationSessionException(
                    "No name specified and there are multiple active sessions. "
                    "Specify a name to create a new session."
                )

            if name in cls._active_sessions:
                return cls._active_sessions[name]
            else:
                return cls(name, **configurations)

    def register_notifier(self, name: str, notifier) -> None:
        self._notifier_manager.register(name, notifier)

    def register_metric_store(self, name: str, metric_store) -> None:
        self._metric_store_manager.register(name, metric_store)

    def register_validation_suite(self, validation_suite: ValidationSuite) -> None:
        if validation_suite.name in self._validation_suites:
            raise ValidationSessionException(
                f"Validation suite '{validation_suite.name}' already exists."
            )
        self._validation_suites[validation_suite.name] = validation_suite

    def run_validation_suite(self, name: str, data: Any) -> None:
        if name not in self._validation_suites:
            raise ValidationSessionException(
                f"Validation suite '{name}' does not exist."
            )
        validation_suite = self._validation_suites[name]
        validation_suite_result = validation_suite.validate(data)

        for metric_store_name in validation_suite.metric_stores:
            self._store_result_metrics(metric_store_name, validation_suite_result)

        for notifier_name in validation_suite.notifiers:
            self._notify(notifier_name, validation_suite_result)

        for check_result in validation_suite_result.check_results:
            if (
                    check_result.status == Status.FAIL
                    and check_result.level == CheckLevel.ERROR
            ):
                raise ValidationSessionException(
                    f"Check '{check_result.name}' failed."
                )

    def _store_result_metrics(
            self,
            metric_store_name: str,
            validation_suite_result: ValidationSuiteResult
    ) -> None:
        if not self._metric_store_manager.exists(metric_store_name):
            self.logger.warning(
                f"Metric store '{metric_store_name}' does not exist.")
            return
        metric_store = self._metric_store_manager.get(metric_store_name)
        try:
            metric_store.save(validation_suite_result)
        except Exception as e:
            self.logger.exception(e)

    def _notify(
            self,
            notifier_name: str,
            validation_suite_result: ValidationSuiteResult
    ) -> None:
        if not self._notifier_manager.exists(notifier_name):
            self.logger.warning(
                f"Notifier '{notifier_name}' does not exist, skipping notification."
            )
            return
        notifier = self._notifier_manager.get(notifier_name)
        try:
            notifier.notify(validation_suite_result)
        except Exception as e:
            self.logger.exception(e)

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, configuration={self._configuration})"