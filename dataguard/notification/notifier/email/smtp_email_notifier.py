from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.validation.result import DataValidationResult


class SMTPEmailNotifier(AbstractNotifier):
    def __init__(self, name: str, disabled: bool = False):
        super().__init__(name, disabled)

    def notify(self, result: DataValidationResult):
        self._logger.info("Sending email notification...")