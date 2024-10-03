class MetricStoreManagerException(Exception):
    pass


class NotifierManagerException(Exception):
    pass


class MetricStoreManager:

    def __init__(self):
        self._metric_store = {}

    def get_metric_store(self, name: str):
        if name in self._metric_store:
            return self._metric_store[name]
        else:
            raise MetricStoreManagerException(f"Metric store '{name}' does not exist.")

    def register_metric_store(self, name: str, metric_store):
        self._metric_store[name] = metric_store

    def remove_metric_store(self, name: str):
        if name in self._metric_store:
            del self._metric_store[name]
        else:
            raise MetricStoreManagerException(f"Metric store '{name}' does not exist.")


class NotifierManager:

    def __init__(self):
        self._notifiers = {}

    def get_notifier(self, name: str):
        if name in self._notifiers:
            return self._notifiers[name]
        else:
            raise NotifierManagerException(f"Notifier '{name}' does not exist.")

    def register_notifier(self, name: str, notifier):
        self._notifiers[name] = notifier

    def remove_notifier(self, name: str):
        if name in self._notifiers:
            del self._notifiers[name]
        else:
            raise NotifierManagerException(f"Notifier '{name}' does not exist.")