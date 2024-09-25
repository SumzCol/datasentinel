from typing import Dict, List, Any

import pandas as pd

from dataguard.validation.result.core import Status, AbstractCheckResult
from dataguard.validation.result.rule_metric import RuleMetric


class PandasCheckResult(AbstractCheckResult):
    def __init__(
            self,
            status: Status,
            rule_metrics: Dict[str, RuleMetric],
            failed_rows: pd.DataFrame | None = None
    ):
        super().__init__(
            status=status,
            rule_metrics=rule_metrics,
        )
        self._failed_rows = failed_rows

    def get_failed_rows(self, top: int = 100) -> List[Dict[str, Any]] | None:
        if self._failed_rows is None:
            return self._failed_rows

        return self._failed_rows.head(top).to_dict(orient="records")