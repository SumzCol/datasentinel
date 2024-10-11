from dataclasses import dataclass
from datetime import datetime
from typing import List

from ulid import ULID

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.status import Status


@dataclass
class ValidationSuiteResult:
    id: ULID
    name: str
    table: str
    schema: str | None
    start_time: datetime
    end_time: datetime
    check_results: List[CheckResult]
    status: Status

    def failed_checks(self, level: CheckLevel | None = None) -> List[CheckResult]:
        if level is None:
            return [check for check in self.check_results if check.status == Status.FAIL]

        return [
            check
            for check in self.check_results
            if check.status == Status.FAIL and check.level == level
        ]

    def failed_checks_name(self, level: CheckLevel | None = None) -> List[str]:
        return [check.name for check in self.failed_checks(level=level)]

    def count_failed_checks(self, level: CheckLevel | None = None) -> int:
        return len(self.failed_checks(level=level))
