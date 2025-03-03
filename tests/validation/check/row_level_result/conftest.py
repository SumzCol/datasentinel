import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.check.level import CheckLevel


@pytest.fixture(scope="function")
def check():
    return RowLevelResultCheck(
        name="row_level_result_check",
        level=CheckLevel.WARNING,
    )
