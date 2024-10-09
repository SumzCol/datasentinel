from typing import Any

from dataguard.validation.check.check import Check
from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.result.check_result import CheckResult


class PrimaryCheck(AbstractCheck):

    def __init__(
            self,
            level: CheckLevel,
            name: str,
            id_column: str
    ):
        super().__init__(level, name)
        self._id_column = id_column

    def check(self, data: Any) -> CheckResult:
        check = Check(
            level=self._level,
            name=self._name,
        )
        return (
            check
            .is_unique(column=self._id_column)
            .is_complete(column=self._id_column)
            .check(data)
        )