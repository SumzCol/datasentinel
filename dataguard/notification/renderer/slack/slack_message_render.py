from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


class SlackMessageRender(AbstractRenderer[str]):
    def render(self, result: DataValidationResult) -> str:
        pass