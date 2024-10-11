from abc import ABC
from typing import Dict

from dataguard.notification.notifier.core import AbstractNotifier


class MetricStoreManagerException(Exception):
    pass


class NotifierManagerException(Exception):
    pass


class MetricStoreManager:

    def __init__(self):
        self._metric_store = {}

    def get(self, name: str):
        if name in self._metric_store:
            return self._metric_store[name]
        else:
            raise MetricStoreManagerException(f"Metric store '{name}' does not exist.")

    def register(self, name: str, metric_store):
        self._metric_store[name] = metric_store

    def remove(self, name: str):
        if name in self._metric_store:
            del self._metric_store[name]
        else:
            raise MetricStoreManagerException(f"Metric store '{name}' does not exist.")

    def exists(self, name: str) -> bool:
        return name in self._metric_store


class NotifierManager:

    def __init__(self):
        self._notifiers: Dict[str, AbstractNotifier] = {}

    def get(self, name: str) -> AbstractNotifier:
        if name in self._notifiers:
            return self._notifiers[name]
        else:
            raise NotifierManagerException(f"Notifier '{name}' does not exist.")

    def register(self, notifier: AbstractNotifier):
        if notifier.name in self._notifiers:
            raise NotifierManagerException(f"Notifier '{notifier.name}' already exists.")
        self._notifiers[notifier.name] = notifier

    def remove(self, name: str):
        if name in self._notifiers:
            del self._notifiers[name]
        else:
            raise NotifierManagerException(f"Notifier '{name}' does not exist.")

    def exists(self, name: str) -> bool:
        return name in self._notifiers