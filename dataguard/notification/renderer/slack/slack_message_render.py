from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.node.result import ValidationNodeResult


class SlackMessageRender(AbstractRenderer[str]):
    def render(self, result: ValidationNodeResult) -> str:
        pass