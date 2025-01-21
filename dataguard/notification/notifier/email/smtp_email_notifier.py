import logging

from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.validation.node.result import ValidationNodeResult


class SMTPEmailNotifier(AbstractNotifier):
    def __init__(self, name: str, disabled: bool = False):
        super().__init__(name, disabled)

    def notify(self, result: ValidationNodeResult):
        self._logger.info("Sending email notification...")