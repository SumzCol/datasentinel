from abc import ABC, abstractmethod
from typing import Dict, List, Any

from dataguard.validation.check.row_level_result.rule import Rule
from dataguard.validation.rule.metric import RuleMetric


class ValidationStrategy(ABC):
    """An interface for validation strategies to adhere to"""

    @abstractmethod
    def validate_data_types(self, df: Any, rules: Dict[str, Rule]) -> bool:
        """Validate that each rule evaluated columns have correct data types

        Args:
            df: The dataframe to validate
            rules: The rules to validate

        Returns:
            Whether the evaluated columns have correct data types
        """

    @abstractmethod
    def compute(self, df: Any, rules: Dict[str, Rule]) -> List[RuleMetric]:
        """Compute and return calculated rule metrics

        Args:
            df: Dataframe to validate
            rules: Rules to compute metrics for

        Returns:
            A list with metrics computed for each rule evaluated
        """
