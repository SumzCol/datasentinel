from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

from ulid import ULID

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.status import Status


@dataclass(frozen=True)
class ValidationNodeResult:
    run_id: ULID
    name: str
    data_asset: str
    data_asset_schema: str | None
    start_time: datetime
    end_time: datetime
    check_results: List[CheckResult]

    @property
    def status(self) -> Status:
        return (
            Status.PASS
            if all([check_result.status == Status.PASS for check_result in self.check_results])
            else Status.FAIL
        )

    @property
    def failed_checks(self) -> List[CheckResult]:
        return [
            check_result
            for check_result in self.check_results
            if check_result.status == Status.FAIL
        ]

    def failed_checks_by_level(self, level: CheckLevel) -> List[CheckResult]:
        return [
            check_result
            for check_result in self.failed_checks
            if check_result.level == level
        ]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": str(self.run_id),
            "name": self.name,
            "data_asset": self.data_asset,
            "data_asset_schema": self.data_asset_schema,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "check_results": [
                check_result.to_dict()
                for check_result in self.check_results
            ],
            "status": self.status.value
        }
