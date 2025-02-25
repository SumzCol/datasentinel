from typing import Any

from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.validation.result import DataValidationResult


class EmailNotifier(AbstractNotifier):
    def __init__(self, name: str, credentials: dict[str, Any], disabled: bool = False):
        self._credentials = credentials
        super().__init__(name, disabled)

    def notify(self, result: DataValidationResult):
        self._logger.info(f"Credentials: {self._credentials}")
        self._logger.info("Sending email!")
