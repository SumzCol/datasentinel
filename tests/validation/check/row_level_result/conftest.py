import pytest

from datasentinel.validation.check import RowLevelResultCheck
from datasentinel.validation.check.level import CheckLevel


@pytest.fixture(scope="function")
def check():
    return RowLevelResultCheck(
        name="row_level_result_check",
        level=CheckLevel.WARNING,
    )
