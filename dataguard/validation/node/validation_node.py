from dataclasses import dataclass
from typing import List, Dict, Any, Set

from dataguard.notification.event import NotifyOnEvent
from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.data_asset.core import AbstractDataAsset


@dataclass
class ValidationNode:
    name: str
    check_list: List[AbstractCheck]
    data_asset: AbstractDataAsset | None = None
    result_stores: List[str] | None = None
    notifiers_by_events: Dict[NotifyOnEvent, List[str]] | None = None
    metadata: Dict[str, Any] | None = None

    def __post_init__(self):
        self.result_stores = self.result_stores or []
        self.notifiers_by_events = self.notifiers_by_events or {}
        self._fill_empty_notify_events()

    def _fill_empty_notify_events(self):
        for enum_value in NotifyOnEvent:
            if enum_value not in self.notifiers_by_events:
                self.notifiers_by_events[enum_value] = []

    @property
    def checks_count(self):
        return len(self.check_list)

    @property
    def has_checks(self) -> bool:
        return self.checks_count > 0

    def add_check(self, check: AbstractCheck):
        self.check_list.append(check)
        return self

    def check_exists(self, check_name: str) -> bool:
        return any(check.name == check_name for check in self.check_list)