import logging
from abc import ABC, abstractmethod
from typing import List

from dataguard.validation.node.result import ValidationNodeResult


class ResultStoreError(Exception):
    pass


class ResultStoreManagerError(Exception):
    pass


class ResultStoreAlreadyExistsError(ResultStoreManagerError):
    pass


class ResultStoreNotFoundError(ResultStoreManagerError):
    pass


class AbstractResultStore(ABC):
    def __init__(self, name: str, disabled: bool):
        self._name = name
        self._disabled = disabled

    @property
    def name(self):
        return self._name

    @property
    def disabled(self):
        return self._disabled

    @abstractmethod
    def store(self, result: ValidationNodeResult):
        """Store a checklist result"""


class AbstractResultStoreManager(ABC):
    """Base class for all result store manager implementations"""
    @property
    def _logger(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @abstractmethod
    def count(self, enabled_only: bool = False) -> int:
        """Return the number of registered result stores

        Args:
            enabled_only: Whether to only consider enabled result stores.
        Returns:
            The number of registered result stores
        """

    @abstractmethod
    def get(self, name: str) -> AbstractResultStore:
        """Get a result store by name

        Args:
            name: Name of the result store

        Returns:
            The result store with the given name

        Raises:
            ResultStoreNotFoundError: When a result store with the given name is not registered
        """

    @abstractmethod
    def register(self, result_store: AbstractResultStore, replace: bool = False):
        """Register a result store

        Args:
            result_store: The result store to register
            replace: Whether to replace an existing result store with the same name or not

        Raises:
            ResultStoreAlreadyExistsError: When the result store already exists and replace
                is False
        """

    @abstractmethod
    def remove(self, name: str):
        """Remove a result store by name

        Args:
            name: Name of the result store to remove

        Raises:
            ResultStoreNotFoundError: When a result store with the given name is not registered
        """

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if a result store with the given name exists

        Args:
            name: Name of the result store to check

        Returns:
            Whether the result store with the given name exists
        """

    @abstractmethod
    def store_all(self, result_stores: List[str], result: ValidationNodeResult):
        """Store a result in all the given result stores

        Args:
            result_stores: A list with the name of the result stores
            result: The result to be stored
        """