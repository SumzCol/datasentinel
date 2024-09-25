from typing import Dict, List, Any

from pyspark.sql import DataFrame

from dataguard.validation.result.core import Status, AbstractCheckResult
from dataguard.validation.result.rule_metric import RuleMetric


class PysparkCheckResult(AbstractCheckResult):

    def __init__(
            self,
            status: Status,
            rule_metrics: Dict[str, RuleMetric],
            failed_rows: DataFrame | None = None
    ):
        super().__init__(
            status=status,
            rule_metrics=rule_metrics,
        )
        self._failed_rows = failed_rows

    def get_failed_rows(self, top: int = 100) -> List[Dict[str, Any]] | None:
        if self._failed_rows is None:
            return self._failed_rows

        return [
            dict(row)
            for row in self._failed_rows.limit(top).collect()
        ]