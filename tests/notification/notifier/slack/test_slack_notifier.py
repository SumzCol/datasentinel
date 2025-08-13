from unittest.mock import Mock, patch

import pytest

from dataguard.notification.notifier.core import NotifierError
from dataguard.notification.notifier.slack import SlackNotifier
from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.notification.renderer.slack.slack_message_renderer import SlackMessage
from dataguard.validation.result import DataValidationResult


@pytest.fixture
def mock_renderer():
    """Fixture to mock the AbstractRenderer."""
    renderer = Mock(spec=AbstractRenderer)
    renderer.render.return_value = SlackMessage(
        blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
        text="Test message",
    )
    return renderer


@pytest.mark.unit
@pytest.mark.notifier
class TestSlackNotifierUnit:
    def test_slack_notifier_initialization_success(self, mock_renderer):
        credentials = {"SLACK_TOKEN": "token"}
        channel = "#test-channel"

        notifier = SlackNotifier(
            name="test-notifier",
            channel=channel,
            credentials=credentials,
            renderer=mock_renderer,
        )

        assert notifier._slack_token == credentials["SLACK_TOKEN"]
        assert notifier._channel == channel
        assert notifier._renderer == mock_renderer

    def test_slack_notifier_initialization_missing_token(self, mock_renderer):
        credentials = {}
        channel = "#test-channel"

        with pytest.raises(NotifierError, match="Slack token not found in credentials."):
            SlackNotifier(
                name="test-notifier",
                channel=channel,
                credentials=credentials,
                renderer=mock_renderer,
            )

    def test_slack_notifier_initialization_missing_channel(self, mock_renderer):
        credentials = {"SLACK_TOKEN": "token"}
        channel = ""

        with pytest.raises(NotifierError, match="Slack channel must be provided."):
            SlackNotifier(
                name="test-notifier",
                channel=channel,
                credentials=credentials,
                renderer=mock_renderer,
            )

    def test_notify_success(self, mock_renderer):
        """Test successful notification."""
        credentials = {"SLACK_TOKEN": "token"}
        channel = "#test-channel"
        notifier = SlackNotifier(
            name="test-notifier",
            channel=channel,
            credentials=credentials,
            renderer=mock_renderer,
        )

        mock_result = Mock(spec=DataValidationResult)

        with patch(
            "dataguard.notification.notifier.slack.slack_notifier.WebClient"
        ) as mock_web_client:
            notifier.notify(mock_result)

            mock_renderer.render.assert_called_once_with(mock_result)
            mock_web_client.assert_called_once_with(token="token")  # noqa S106
            mock_web_client.return_value.chat_postMessage.assert_called_once_with(
                channel=channel,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "Test message"}}],
                text="Test message",
            )

    def test_notify_failure(self, mock_renderer):
        credentials = {"SLACK_TOKEN": "token"}
        channel = "#test-channel"
        notifier = SlackNotifier(
            name="test-notifier",
            channel=channel,
            credentials=credentials,
            renderer=mock_renderer,
        )

        mock_result = Mock(spec=DataValidationResult)

        with patch(
            "dataguard.notification.notifier.slack.slack_notifier.WebClient"
        ) as mock_web_client:
            mock_web_client.return_value.chat_postMessage.side_effect = Exception("Mock Error")

            with pytest.raises(
                NotifierError, match="Error while sending slack message: Mock Error"
            ):
                notifier.notify(mock_result)

            mock_renderer.render.assert_called_once_with(mock_result)
            mock_web_client.assert_called_once_with(token="token")  # noqa S106
            mock_web_client.return_value.chat_postMessage.assert_called_once()
