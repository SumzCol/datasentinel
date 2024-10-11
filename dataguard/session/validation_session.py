import logging
import threading
from typing import Union, Any, Dict, List

from dataguard.notification.notifier.exceptions import NotificationException
from dataguard.notification.notify_event import NotifyOnEvent
from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.session.core import ValidationSessionException
from dataguard.session.manager import NotifierManager, MetricStoreManager
from dataguard.session.session_configuration import SessionConfiguration
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.exceptions import FailedCheckException
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

    def register_notifier(self, notifier: AbstractNotifier) -> None:
        self._notifier_manager.register(notifier)

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

        if validation_suite_result.status == Status.PASS:
            self.logger.info(
                f"Validation suite '{name}' passed on table '{validation_suite.table}'."
            )
        else:
            self.logger.error(
                f"Validation suite '{name}' failed on table '{validation_suite.table}'. "
                f"Failed checks: {','.join(validation_suite_result.failed_checks_name())}"
            )

        self._store_result_metrics(validation_suite, validation_suite_result)

        self._notify(validation_suite, validation_suite_result)


        if validation_suite_result.count_failed_checks(level=CheckLevel.ERROR) > 0:
            failed_checks = validation_suite_result.failed_checks_name(level=CheckLevel.ERROR)
            raise FailedCheckException(
                f"Error level checks ({','.join(failed_checks)}) "
                f"failed in validation suite '{name}' "
                f"on table '{validation_suite.table}'."
            )

    def _store_result_metrics(
            self,
            validation_suite: ValidationSuite,
            validation_suite_result: ValidationSuiteResult
    ) -> None:
        for metric_store_name in validation_suite.metric_stores:
            if not self._metric_store_manager.exists(metric_store_name):
                self.logger.warning(
                    f"Metric store '{metric_store_name}' does not exist.")
                return
            metric_store = self._metric_store_manager.get(metric_store_name)
            try:
                metric_store.store(validation_suite_result)
            except Exception as e:
                self.logger.exception(e)

    def _notify(
            self,
            validation_suite: ValidationSuite,
            validation_suite_result: ValidationSuiteResult
    ) -> None:
        if validation_suite_result.status == Status.FAIL:
            self._send_notifications(
                notifiers=validation_suite.get_notifiers_by_event(NotifyOnEvent.FAILURE),
                validation_suite_result=validation_suite_result
            )
        elif validation_suite_result.status == Status.PASS:
            self._send_notifications(
                notifiers=validation_suite.get_notifiers_by_event(NotifyOnEvent.SUCCESS),
                validation_suite_result=validation_suite_result
            )
        self._send_notifications(
            notifiers=validation_suite.get_notifiers_by_event(NotifyOnEvent.ALL),
            validation_suite_result=validation_suite_result
        )

    def _send_notifications(
            self,
            notifiers: List[str],
            validation_suite_result: ValidationSuiteResult
    ) -> None:
        for notifier_name in notifiers:
            if not self._notifier_manager.exists(notifier_name):
                self.logger.warning(
                    f"Notifier '{notifier_name}' does not exist, skipping notification."
                )
                return
            notifier = self._notifier_manager.get(notifier_name)

            if notifier.disabled:
                self.logger.warning(
                    f"Notifier '{notifier_name}' is disabled, skipping notification."
                )
                return
            try:
                notifier.notify(validation_suite_result)
            except NotificationException as e:
                self.logger.error(
                    f"Failed to send notification via notifier '{notifier_name}': {str(e)}"
                )

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, conf={self._configuration})"