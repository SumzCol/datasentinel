from unittest.mock import Mock

import pytest

from dataguard.notification.notifier.email.smtp_email_notifier import SMTPEmailNotifier
from dataguard.notification.renderer.core import AbstractRenderer


@pytest.mark.unit
class TestSMTPEmailNotifierUnit:
    def test_name_property(self):
        expected_name = "test"
        notifier = SMTPEmailNotifier(
            name=expected_name,
            server="test_server",
            port=25,
            domain="test_domain",
            renderer=Mock(spec=AbstractRenderer),
            credentials={"username": "test_username", "password": "test_password"},
        )

        assert notifier.name == expected_name

    def test_disabled_property(self):
        disabled = True
        notifier = SMTPEmailNotifier(
            name="test",
            server="test_server",
            port=25,
            domain="test_domain",
            renderer=Mock(spec=AbstractRenderer),
            credentials={"username": "test_username", "password": "test_password"},
            disabled=disabled,
        )

        assert notifier.disabled == disabled
