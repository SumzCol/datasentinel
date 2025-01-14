import logging
from typing import Dict, Any

from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.validation.node.result import ValidationNodeResult


class EmailNotifier(AbstractNotifier):
    def __init__(self, name: str, credentials: Dict[str, Any], disabled: bool = False):
        self._credentials = credentials
        super().__init__(name, disabled)

    @property
    def logger(self):
        return logging.getLogger(__name__)

    def notify(self, result: ValidationNodeResult):
        self.logger.info(f"Credentials: {self._credentials}")
        self.logger.info(f"Sending email!")
