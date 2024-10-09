from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List

from ulid import ULID

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

    def failed_checks(self) -> List[CheckResult]:
        return [check for check in self.check_results if check.status == Status.FAIL]

    def count_failed_checks(self) -> int:
        return len(self.failed_checks())
