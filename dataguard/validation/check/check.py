from typing import Any

from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.level import CheckLevel


class Check(AbstractCheck):

    def __init__(self, level: CheckLevel, name: str):
        super().__init__(level, name)

    def is_complete(self, column: str):
        return self

    def is_unique(self, column: str):
        return self

    def check(self, data: Any) -> CheckResult:
        pass