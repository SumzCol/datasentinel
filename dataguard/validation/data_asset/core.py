from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')

class AbstractDataAsset(ABC, Generic[T]):
    def __init__(self, name: str, schema: str | None = None):
        self._name = name
        self._schema = schema

    @property
    def name(self) -> str:
        """Return the name of the data asset"""
        return self._name

    @property
    def schema(self) -> str | None:
        """Return the schema of the data asset"""
        return self._schema

    @abstractmethod
    def load(self) -> T:
        """Load the data asset"""