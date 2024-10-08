from typing import Dict, Any, List

from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.result import ValidationSuiteResult


class ValidationSuite:
    def __init__(
            self,
            name: str,
            table_name: str,
            schema_name: str | None = None,
            metadata: Dict[str, Any] | None = None,
            check_list: List[AbstractCheck] | None = None,
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

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def schema_name(self) -> str | None:
        return self._schema_name

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata

    @property
    def metric_stores(self) -> List[str]:
        return self._metric_stores

    @property
    def notifiers(self) -> List[str]:
        return self._notifiers

    def add_metric_store(self, name: str):
        self._metric_stores.append(name)

    def add_notifier(self, name: str):
        self._notifiers.append(name)

    def add_check(self, check: AbstractCheck):
        self._check_list.append(check)

    def validate(self, data: Any) -> ValidationSuiteResult:
        for check in self._check_list:
            check_result = check.check(data)