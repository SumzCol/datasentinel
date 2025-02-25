from email.message import Message

from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


class EmailMessageRenderer(AbstractRenderer[Message]):
    def render(self, result: DataValidationResult) -> Message:
        pass
