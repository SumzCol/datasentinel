from dataclasses import dataclass
from typing import Dict

from dataguard.validation.result.core import Status, AbstractCheckResult


@dataclass
class ValidationResult:
    status: Status
    check_results: Dict[str, AbstractCheckResult]
