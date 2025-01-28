from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


class EmailHTMLRenderer(AbstractRenderer[str]):
    def render(self, result: DataValidationResult) -> str:
        pass