from typing import List, Dict, Any

from pandas import DataFrame


class PandasBadRecordsDataset:
    def __init__(self, raw_data: DataFrame):
        self._raw_data = raw_data

    def count(self) -> int:
        return self._raw_data.shape[0]

    def to_dict(self, top: int = 1000) -> List[Dict[str, Any]]:
        return self._raw_data.head(top).to_dict(orient="records")
