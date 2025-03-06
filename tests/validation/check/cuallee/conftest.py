import pytest

from dataguard.validation.check import CualleeCheck
from dataguard.validation.check.level import CheckLevel


@pytest.fixture(scope="function")
def check():
    return CualleeCheck(
        name="cuallee_check",
        level=CheckLevel.WARNING,
    )
