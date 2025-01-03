from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')

class AbstractDatasource(ABC, Generic[T]):
    def __init__(self, data_asset: str, data_asset_schema: str | None = None):
        self._data_asset = data_asset
        self._data_asset_schema = data_asset_schema

    @property
    def data_asset(self) -> str:
        """Return the name of the data asset"""
        return self._data_asset

    @property
    def data_asset_schema(self) -> str | None:
        """Return the schema of the data asset"""
        return self._data_asset_schema

    @abstractmethod
    def load(self) -> T:
        """Load the data asset"""