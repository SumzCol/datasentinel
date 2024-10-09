class MetricStoreManager:

    def __init__(self):
        self._metric_store = {}

    def get_metric_store(self, name: str):
        if name in self._metric_store:
            return self._metric_store[name]
        else:
            raise Exception(f"Metric store '{name}' does not exist.")

    def register_metric_store(self, name: str, metric_store):
        self._metric_store[name] = metric_store