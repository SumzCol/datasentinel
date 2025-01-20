import threading
from typing import Dict, List

from dataguard.validation.node.core import NotifyOnEvent
from dataguard.notification.notifier.core import (
    AbstractNotifier,
    NotifierAlreadyExistsError,
    NotifierNotFoundError, AbstractNotifierManager,
)
from dataguard.store.result.core import ResultStoreError
from dataguard.validation.node.result import ValidationNodeResult
from dataguard.validation.status import Status


class NotifierManager(AbstractNotifierManager):
    _lock = threading.Lock()

    def __init__(self):
        self._notifiers: Dict[str, AbstractNotifier] = {}

    def count(self, enabled_only: bool = False) -> int:
        return len([
            notifier
            for notifier in self._notifiers.values()
            if not enabled_only or (enabled_only and not notifier.disabled)
        ])

    def get(self, name: str) -> AbstractNotifier:
        if not self.exists(name):
            raise NotifierNotFoundError(f"Notifier '{name}' does not exist.")
        return self._notifiers[name]

    def register(self, notifier: AbstractNotifier, replace: bool = False):
        if notifier.name in self._notifiers and not replace:
            raise NotifierAlreadyExistsError(
                f"Notifier with name '{notifier.name}' already exists."
            )
        with self._lock:
            self._notifiers[notifier.name] = notifier

    def remove(self, name: str):
        if not self.exists(name):
            raise NotifierNotFoundError(f"Notifier '{name}' does not exist.")
        with self._lock:
            del self._notifiers[name]

    def exists(self, name: str) -> bool:
        return name in self._notifiers

    def notify_all_by_event(
        self,
        notifiers_by_events: Dict[NotifyOnEvent, List[str]],
        result: ValidationNodeResult
    ):
        status = result.status
        notifiers = []
        if status == Status.PASS:
            notifiers.extend(notifiers_by_events.get(NotifyOnEvent.PASS, []))
        else:
            notifiers.extend(notifiers_by_events.get(NotifyOnEvent.FAIL, []))

        notifiers.extend(notifiers_by_events.get(NotifyOnEvent.ALL, []))

        for notifier in notifiers:
            self._notify(
                notifier=self.get(notifier),
                result=result
            )

    def _notify(
        self,
        notifier: AbstractNotifier,
        result: ValidationNodeResult
    ):
        if notifier.disabled:
            self._logger.warning(
                f"Notifier '{notifier.name}' is disabled, skipping sending notification."
            )
            return
        try:
            notifier.notify(result=result)
        except ResultStoreError as e:
            self._logger.error(
                f"There was an error while trying to send notification "
                f"using notifier '{notifier.name}'. Error: {str(e)}"
            )
