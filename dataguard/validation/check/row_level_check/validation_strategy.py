from abc import ABC, abstractmethod
from typing import Dict, List, Any

from dataguard.validation.check.row_level_check.rule import Rule
from dataguard.validation.rule.metric import RuleMetric


class ValidationStrategy(ABC):
    """An interface for validation strategies to adhere to"""
    @abstractmethod
    def validate_data_types(self, dataframe: Any, rules: Dict[str, Rule]) -> bool:
        """Validate that each rule evaluated columns have correct data types"""

    @abstractmethod
    def compute(self, dataframe: Any, rules: Dict[str, Rule]) -> List[RuleMetric]:
        """Compute and return calculated rule metrics"""