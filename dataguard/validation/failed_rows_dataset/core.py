from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class AbstractFailedRowsDataset(ABC, Generic[T]):
    """Base class for all bad records dataset implementations"""

    def __init__(self, data: T):
        self._data = data

    @property
    def data(self) -> T:
        """Return bad records in their native format"""
        return self._data

    @abstractmethod
    def count(self) -> int:
        """Returns the number of bad records"""

    @abstractmethod
    def to_dict(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Returns bad records as a list of dictionaries

        Args:
            limit: Limits the number of bad records to return
        """

    @abstractmethod
    def to_json(self, limit: int | None = None) -> str:
        """
        Returns bad records as JSON string

        Args:
            limit: Limits the number of bad records to return
        """
