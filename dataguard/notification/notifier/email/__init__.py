"""`dataguard.notification.notifier.email` contains all email notifiers."""

from .smtp_email_notifier import SMTPEmailNotifier


__all__ = ["SMTPEmailNotifier"]
