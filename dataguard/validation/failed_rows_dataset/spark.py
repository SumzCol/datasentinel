import json
from typing import Any

from pyspark.sql import DataFrame

from dataguard.validation.failed_rows_dataset.core import AbstractFailedRowsDataset


class SparkFailedRowsDataset(AbstractFailedRowsDataset[DataFrame]):
    def __init__(self, data: DataFrame):
        super().__init__(data)

    def count(self) -> int:
        return self._data.count()

    def to_dict(self, limit: int | None = None) -> list[dict[str, Any]]:
        if limit is not None and not limit > 0:
            raise ValueError("Limit must be greater than 0")

        data = self._data.limit(limit) if limit is not None else self._data
        return [row.asDict() for row in data.collect()]

    def to_json(self, limit: int | None = None) -> str:
        return json.dumps(self.to_dict(limit))
