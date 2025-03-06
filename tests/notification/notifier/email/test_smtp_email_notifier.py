from email.message import EmailMessage
from unittest.mock import MagicMock, Mock, patch

import pytest

from dataguard.notification.notifier.core import NotifierError
from dataguard.notification.notifier.email import SMTPEmailNotifier
from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult


@pytest.fixture
def mock_renderer():
    renderer = Mock(spec=AbstractRenderer)
    renderer.render.return_value = EmailMessage()
    return renderer


@pytest.mark.unit
class TestSMTPEmailNotifierUnit:
    def test_initialization_success(self, mock_renderer):
        credentials = {"username": "user@example.com", "password": "password123"}
        notifier = SMTPEmailNotifier(
            name="test-notifier",
            server="smtp.example.com",
            port=587,
            domain="example.com",
            renderer=mock_renderer,
            credentials=credentials,
            mail_rcp=["recipient@example.com"],
            mail_rcp_cc=["cc@example.com"],
        )

        assert notifier._server == "smtp.example.com"
        assert notifier._port == 587  # noqa PLR2004
        assert notifier._mail_rcp == ["recipient@example.com"]
        assert notifier._mail_rcp_cc == ["cc@example.com"]
        assert notifier._domain == "example.com"
        assert notifier._username == "user@example.com"
        assert notifier._password == "password123"  # noqa S105
        assert notifier._renderer == mock_renderer

    def test_initialization_missing_credentials(self, mock_renderer):
        credentials = {}

        with pytest.raises(NotifierError, match="Username or password not found in credentials."):
            SMTPEmailNotifier(
                name="test-notifier",
                server="smtp.example.com",
                port=587,
                domain="example.com",
                renderer=mock_renderer,
                credentials=credentials,
            )

    def test_notify_success(self, mock_renderer):
        credentials = {"username": "user@example.com", "password": "password123"}
        notifier = SMTPEmailNotifier(
            name="test-notifier",
            server="smtp.example.com",
            port=587,
            domain="example.com",
            renderer=mock_renderer,
            credentials=credentials,
            mail_rcp=["recipient@example.com"],
            mail_rcp_cc=["cc@example.com"],
        )

        mock_result = MagicMock(spec=DataValidationResult)

        with patch("dataguard.notification.notifier.email.smtp_email_notifier.SMTP") as mock_smtp:
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance

            notifier.notify(mock_result)

            mock_renderer.render.assert_called_once_with(mock_result)
            mock_smtp.assert_called_once_with("smtp.example.com", 587)
            mock_smtp_instance.ehlo.assert_called_with("example.com")
            mock_smtp_instance.starttls.assert_called_once()
            mock_smtp_instance.login.assert_called_once_with("user@example.com", "password123")
            mock_smtp_instance.send_message.assert_called_once()
            mock_smtp_instance.quit.assert_called_once()

    def test_notify_failure(self, mock_renderer):
        credentials = {"username": "user@example.com", "password": "password123"}
        notifier = SMTPEmailNotifier(
            name="test-notifier",
            server="smtp.example.com",
            port=587,
            domain="example.com",
            renderer=mock_renderer,
            credentials=credentials,
            mail_rcp=["recipient@example.com"],
        )

        mock_result = MagicMock(spec=DataValidationResult)

        with patch("dataguard.notification.notifier.email.smtp_email_notifier.SMTP") as mock_smtp:
            mock_smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_smtp_instance
            mock_smtp_instance.login.side_effect = Exception("Mock Exception")

            with pytest.raises(
                NotifierError,
                match="Error while sending email: Mock Exception",
            ):
                notifier.notify(mock_result)

            mock_renderer.render.assert_called_once_with(mock_result)
            mock_smtp.assert_called_once_with("smtp.example.com", 587)
            mock_smtp_instance.ehlo.assert_called_with("example.com")
            mock_smtp_instance.starttls.assert_called_once()
            mock_smtp_instance.login.assert_called_once_with("user@example.com", "password123")
            mock_smtp_instance.send_message.assert_not_called()
            mock_smtp_instance.quit.assert_not_called()
