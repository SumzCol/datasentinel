import logging
from typing import Dict, Any

from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.validation.result import DataValidationResult


class EmailNotifier(AbstractNotifier):
    def __init__(self, name: str, credentials: Dict[str, Any], disabled: bool = False):
        self._credentials = credentials
        super().__init__(name, disabled)

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def notify(self, result: DataValidationResult):
        self.logger.info(f"Credentials: {self._credentials}")
        self.logger.info("Sending email!")
