import smtplib
from email.message import EmailMessage
from typing import Any

from dataguard.notification.notifier.core import AbstractNotifier, NotifierError
from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


class SMTPEmailNotifier(AbstractNotifier):
    def __init__(  # noqa PLR0913
        self,
        name: str,
        server: str,
        port: int,
        domain: str,
        renderer: AbstractRenderer[EmailMessage],
        credentials: dict[str, Any],
        mail_rcp: list[str] | None = None,
        mail_rcp_cc: list[str] | None = None,
        disabled: bool = False,
    ):
        super().__init__(name, disabled)
        self._server = server
        self._port = port
        self._mail_rcp = mail_rcp if mail_rcp else []
        self._mail_rcp_cc = mail_rcp_cc if mail_rcp_cc else []
        self._domain = domain
        self._renderer = renderer

        if "username" not in credentials or "password" not in credentials:
            raise NotifierError("Username or password not found in credentials.")

        self._username = credentials["username"]
        self._password = credentials["password"]

    def _define_recipients(self, message: EmailMessage):
        message["From"] = self._username
        message["To"] = ", ".join(self._mail_rcp)
        message["BCC"] = ", ".join(self._mail_rcp)
        if self._mail_rcp_cc:
            message["CC"] = ", ".join(self._mail_rcp_cc)

        return message

    def notify(self, result: DataValidationResult):
        message = self._define_recipients(self._renderer.render(result))

        try:
            with smtplib.SMTP(self._server, self._port) as server:
                server.ehlo(self._domain)
                server.starttls()
                server.ehlo(self._domain)
                server.login(self._username, self._password)
                server.send_message(message)
                server.quit()
        except smtplib.SMTPException as e:
            raise NotifierError(f"Error while sending email: {e!s}") from e
