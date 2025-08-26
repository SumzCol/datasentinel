from typing import Any

import lazy_loader as lazy


try:
    from .cuallee import CualleeCheck  # noqa: F401
except (ImportError, RuntimeError):
    CualleeCheck: Any

try:
    from .row_level_result import RowLevelResultCheck  # noqa: F401
except (ImportError, RuntimeError):
    RowLevelResultCheck: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "cuallee": ["CualleeCheck"],
        "row_level_result": ["RowLevelResultCheck"],
    },
)
