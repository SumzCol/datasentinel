from typing import List, Dict, Any

from pyspark.sql import DataFrame

from dataguard.validation.result.core import AbstractBadRecordsDataset


class SparkBadRecordsDataset(AbstractBadRecordsDataset):
    def __init__(self, raw_data: DataFrame):
        self._raw_data = raw_data

    def count(self) -> int:
        return self._raw_data.count()

    def to_dict(self, top: int = 1000) -> List[Dict[str, Any]]:
        return [
            dict(row)
            for row in self._raw_data.limit(top).collect()
        ]
