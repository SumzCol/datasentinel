import enum
import re
from typing import Any

from toolz import first

from dataguard.validation.check.utils import get_type


class DataframeType(enum.Enum):
    PYSPARK = "pyspark"
    PANDAS = "pandas"

    @classmethod
    def from_df(cls, df: Any) -> "DataframeType":
        _type = get_type(df)

        if "pyspark" in _type:
            return DataframeType.PYSPARK
        elif "pandas" in _type:
            return DataframeType.PANDAS
        else:
            raise ValueError(f"Unsupported dataframe type: {_type}")
