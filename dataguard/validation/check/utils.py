import re
from typing import Any

from toolz import first


def _get_df_type(df: Any) -> str:
    return first(re.match(r".*'(.*)'", str(type(df))).groups())
