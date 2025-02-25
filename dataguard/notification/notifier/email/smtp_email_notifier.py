from email.message import Message

from dataguard.notification.notifier.core import AbstractNotifier
from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


class SMTPEmailNotifier(AbstractNotifier):
    def __init__(  # noqa PLR0913
        self,
        name: str,
        server: str,
        port: int,
        domain: str,
        renderer: AbstractRenderer[Message],
        mails_rcp: list[str] | None = None,
        mails_rcp_cc: list[str] | None = None,
        credentials: dict[str, any] | None = None,
        disabled: bool = False,
    ):
        super().__init__(name, disabled)
        self._server = server
        self._port = port
        self._mails_rcp = mails_rcp if mails_rcp else []
        self._mails_rcp_cc = mails_rcp_cc if mails_rcp_cc else []
        self._domain = domain
        self._credentials = credentials if credentials else {}
        self._renderer = renderer

    def notify(self, result: DataValidationResult):
        self._logger.info("Sending email notification...")
