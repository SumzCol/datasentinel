import enum
from typing import Any

from pyspark.sql.functions import typeof

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
            raise ValueError(f"{type(df)} is not a valid dataframe type")
