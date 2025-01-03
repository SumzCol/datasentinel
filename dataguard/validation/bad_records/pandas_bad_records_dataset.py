import json
from typing import List, Dict, Any

from pandas import DataFrame

from dataguard.validation.bad_records.core import AbstractBadRecordsDataset


class PandasBadRecordsDataset(AbstractBadRecordsDataset[DataFrame]):
    def __init__(self, data: DataFrame):
        super().__init__(data)

    @property
    def data(self) -> DataFrame:
        return self._data

    def count(self) -> int:
        return self._data.shape[0]

    def to_dict(self, limit: int = None) -> List[Dict[str, Any]]:
        if limit is not None and not limit > 0:
            raise ValueError("Limit must be greater than 0")

        data = self._data.head(limit) if limit is not None else self._data
        return data.to_dict(orient="records")

    def to_json(self, limit: int = None) -> str:
        return json.dumps(self.to_dict(limit))
