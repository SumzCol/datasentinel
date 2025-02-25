from email.message import EmailMessage

from dataguard.notification.renderer.core import AbstractRenderer, RendererError
from dataguard.validation.result import DataValidationResult


class EmailMessageRenderer(AbstractRenderer[EmailMessage]):
    def __init__(self, include_failed_rows: bool = False, failed_rows_limit: int = 100):
        if not failed_rows_limit > 0:
            raise RendererError("Failed rows limit must be greater than 0")
        self._include_failed_records = include_failed_rows
        self._failed_rows_limit = failed_rows_limit

    def render(self, result: DataValidationResult) -> EmailMessage:
        pass
