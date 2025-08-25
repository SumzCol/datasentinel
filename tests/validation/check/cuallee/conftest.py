import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.check.level import CheckLevel


@pytest.fixture(scope="function")
def check():
    return CualleeCheck(
        name="cuallee_check",
        level=CheckLevel.WARNING,
    )
