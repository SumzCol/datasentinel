from typing import Dict, Any, List

from dataguard.validation.result import ValidationSuiteResult


class ValidationSuite:
    def __init__(
            self,
            name: str,
            table_name: str,
            schema_name: str | None = None,
            metadata: Dict[str, Any] | None = None,
            check_list: Dict[str, Any] | None = None,
            metric_stores: List[str] | None = None,
            notifier: List[str] | None = None,
    ):
        self._name = name
        self._table_name = table_name
        self._schema_name = schema_name
        self._metadata = {} if metadata is None else metadata
        self._check_list = {} if check_list is None else check_list
        self._metric_stores = [] if metric_stores is None else metric_stores
        self._notifiers = [] if notifier is None else notifier

    @property
    def name(self) -> str:
        return self._name

    def add_metric_store(self, name: str):
        self._metric_stores.append(name)

    def add_notifier(self, name: str):
        self._notifiers.append(name)

    def validate(self, data: Any) -> ValidationSuiteResult:
        pass