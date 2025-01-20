import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from dataguard.validation.node.core import NotifyOnEvent
from dataguard.validation.node.result import ValidationNodeResult


class NotifierError(Exception):
    pass


class NotifierManagerError(Exception):
    pass


class NotifierAlreadyExistsError(NotifierManagerError):
    pass


class NotifierNotFoundError(NotifierManagerError):
    pass


class AbstractNotifier(ABC):

    def __init__(self, name: str, disabled: bool = False):
        self._name = name
        self._disabled = disabled

    @property
    def name(self) -> str:
        return self._name

    @property
    def disabled(self) -> bool:
        return self._disabled

    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @abstractmethod
    def notify(self, result: ValidationNodeResult):
        pass


class AbstractNotifierManager(ABC):
    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @abstractmethod
    def count(self, enabled_only: bool = False) -> int:
        """Return the number of registered notifiers

        Args:
            enabled_only: Whether to only consider enabled notifiers.
        Returns:
            The number of registered notifiers
        """

    @abstractmethod
    def get(self, name: str) -> AbstractNotifier:
        """Get notifier by name

        Args:
            name: Notifier name

        Returns:
            AbstractNotifier: Notifier instance with the given name
        """

    @abstractmethod
    def register(self, notifier: AbstractNotifier, replace: bool = False):
        """Register notifier

        Args:
            notifier: Notifier to register
            replace: Whether to replace an existing notifier if it already exists

        Raises:
            NotifierAlreadyExistsError: When the notifier already exists and replace is False
        """

    @abstractmethod
    def remove(self, name: str):
        """Remove notifier by name

        Args:
            name: Name of the notifier to be removed

        Raises:
            NotifierNotFoundError: When a notifier with the given name was not registered before
        """

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if notifier exists

        Args:
            name: Name of the notifier to check if its registered

        Returns:
            whether the notifier exists
        """

    @abstractmethod
    def notify_all_by_event(
            self,
            notifiers_by_events: Dict[NotifyOnEvent, List[str]],
            result: ValidationNodeResult
    ):
        """
        Notify a validation node result using the specified notifiers for each event.

        Args:
            notifiers_by_events: A dictionary where each key is
                an event, and the corresponding value is a list of the notifiers name to be used
                when that event occurs.
            result: The validation node result to be notified.
        """