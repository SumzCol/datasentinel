from typing import Any

from dataguard.validation.datasource.core import AbstractDatasource


class MemoryDatasource(AbstractDatasource[Any]):
    def __init__(self, data: Any, data_asset: str, data_asset_schema: str | None = None):
        self._data = data
        super().__init__(data_asset=data_asset, data_asset_schema=data_asset_schema)

    def load(self) -> Any:
        if self._data is None:
            raise ValueError("Data for MemoryDatasource has not been saved yet.")
        return self._data
