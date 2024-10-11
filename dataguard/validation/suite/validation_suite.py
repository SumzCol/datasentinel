from typing import Dict, Any, List

from dataguard.notification.notify_event import NotifyOnEvent
from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.suite.result import ValidationSuiteResult


class ValidationSuite:
    def __init__(
            self,
            name: str,
            table: str,
            schema: str | None = None,
            metadata: Dict[str, Any] | None = None,
            check_list: List[AbstractCheck] | None = None,
            metric_stores: List[str] | None = None,
            on_failure_notifiers: List[str] | None = None,
            on_success_notifiers: List[str] | None = None,
            on_all_notifiers: List[str] | None = None,
    ):
        self._name = name
        self._table = table
        self._schema = schema
        self._metadata = {} if metadata is None else metadata
        self._check_list = {} if check_list is None else check_list
        self._metric_stores = [] if metric_stores is None else metric_stores
        self._on_failure_notifiers = (
            [] if on_failure_notifiers is None else on_failure_notifiers
        )
        self._on_success_notifiers = (
            [] if on_success_notifiers is None else on_success_notifiers
        )
        self._on_all_notifiers = [] if on_all_notifiers is None else on_all_notifiers

    @property
    def name(self) -> str:
        return self._name

    @property
    def table(self) -> str:
        return self._table

    @property
    def schema(self) -> str | None:
        return self._schema

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata

    @property
    def metric_stores(self) -> List[str]:
        return self._metric_stores

    def add_metric_store(self, name: str):
        self._metric_stores.append(name)

    def add_notifier(self, name: str, on_event: NotifyOnEvent):
        if on_event == NotifyOnEvent.SUCCESS:
            self._on_success_notifiers.append(name)
        elif on_event == NotifyOnEvent.FAILURE:
            self._on_failure_notifiers.append(name)
        elif on_event == NotifyOnEvent.ALL:
            self._on_all_notifiers.append(name)
        else:
            raise ValueError(f"Invalid notifier event: {on_event}")

    def get_notifiers_by_event(self, on_event: NotifyOnEvent) -> List[str]:
        if on_event == NotifyOnEvent.SUCCESS:
            return self._on_success_notifiers
        elif on_event == NotifyOnEvent.FAILURE:
            return self._on_failure_notifiers
        elif on_event == NotifyOnEvent.ALL:
            return self._on_all_notifiers
        else:
            raise ValueError(f"Invalid notifier event: {on_event}")

    def add_check(self, check: AbstractCheck):
        self._check_list.append(check)

    def validate(self, data: Any) -> ValidationSuiteResult:
        pass