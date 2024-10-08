from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

from dataguard.validation.result.check_result import CheckResult
from dataguard.validation.result.core import Status


@dataclass
class ValidationSuiteResult:
    id: str
    name: str
    table_name: str
    schema_name: str | None
    metadata: Dict[str, Any] | None
    start_time: datetime
    end_time: datetime
    check_results: List[CheckResult]
    status: Status
