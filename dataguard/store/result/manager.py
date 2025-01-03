import logging
import threading
from typing import Dict, List

from dataguard.notification.notifier.core import NotifierError
from dataguard.store.result.core import (
    AbstractResultStore,
    ResultStoreNotFoundError,
    ResultStoreAlreadyExistsError, AbstractResultStoreManager,
)
from dataguard.validation.node.result import ValidationNodeResult


class ResultStoreManager(AbstractResultStoreManager):
    _lock = threading.Lock()
    def __init__(self):
        self._metric_stores: Dict[str, AbstractResultStore] = {}

    @property
    def count(self) -> int:
        return len(self._metric_stores)

    def get(self, name: str) -> AbstractResultStore:
        if not self.exists(name):
            raise ResultStoreNotFoundError(f"A metric store with '{name}' does not exist.")
        return self._metric_stores[name]

    def register(self, result_store: AbstractResultStore, replace: bool = False):
        if self.exists(result_store.name) and not replace:
            raise ResultStoreAlreadyExistsError(
                f"A metric store with name '{result_store.name}' already exists."
            )
        with self._lock:
            self._metric_stores[result_store.name] = result_store

    def remove(self, name: str):
        if not self.exists(name):
            raise ResultStoreNotFoundError(f"A metric store with name '{name}' does not exist.")
        with self._lock:
            del self._metric_stores[name]

    def exists(self, name: str) -> bool:
        return name in self._metric_stores

    def store_all(self, result_stores: List[str], result: ValidationNodeResult):
        for result_store in result_stores:
            self._store(
                result_store=self.get(result_store),
                result=result
            )

    def _store(
        self,
        result_store: AbstractResultStore,
        result: ValidationNodeResult
    ):
        if result_store.disabled:
            self._logger.warning(
                f"Result store '{result_store.name}' is disabled, skipping saving results."
            )
            return
        try:
            result_store.store(result=result)
        except NotifierError as e:
            self._logger.error(
                f"There was an error while trying to save results "
                f"using store '{result_store.name}'. Error: {str(e)}"
            )
