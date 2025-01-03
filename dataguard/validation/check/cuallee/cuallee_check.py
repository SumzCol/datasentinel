import re
from datetime import datetime
from typing import Any, List, Tuple, Dict, Callable

from toolz import first

from cuallee import Check, CheckLevel as CualleeCheckLevel

from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.check.utils import _get_df_type
from dataguard.validation.rule.metric import RuleMetric


def _cuallee_check_level(level: CheckLevel) -> CualleeCheckLevel:
    return CualleeCheckLevel.WARNING if level.WARNING else CualleeCheckLevel.ERROR


class CualleeCheck(AbstractCheck):

    def __init__(self, level: CheckLevel, name: str):
        self._check = Check(
            level=CualleeCheckLevel(_cuallee_check_level(level=level)),
            name=name
        )
        super().__init__(level, name)

    def is_complete(self, column: str, pct: float = 1.0):
        self._check.is_complete(column=column, pct=pct)
        return self

    def is_empty(self, column: str, pct: float = 1.0):
        self._check.is_empty(column=column, pct=pct)
        return self

    def are_complete(self, column: List[str], pct: float = 1.0):
        self._check.are_complete(column=column, pct=pct)
        return self

    def is_unique(
            self,
            column: str,
            pct: float = 1.0,
            approximate: bool = False,
            ignore_nulls: bool = False
    ):
        self._check.is_unique(
            column=column,
            pct=pct,
            approximate=approximate,
            ignore_nulls=ignore_nulls
        )
        return self

    def is_primary_key(self, column: str, pct: float = 1.0):
        self._check.is_primary_key(column=column, pct=pct)
        return self

    def are_unique(
            self,
            column: List[str],
            pct: float = 1.0,
    ):
        self._check.are_unique(column=column, pct=pct)
        return self

    def is_composite_key(self, column: List[str], pct: float = 1.0):
        self._check.is_composite_key(column=column, pct=pct)
        return self

    def is_greater_than(self, column: str, value: float, pct: float = 1.0):
        self._check.is_greater_than(column=column, value=value, pct=pct)
        return self

    def is_positive(self, column: str, pct: float = 1.0):
        self._check.is_positive(column=column, pct=pct)
        return self

    def is_greater_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        self._check.is_greater_or_equal_than(column=column, value=value, pct=pct)
        return self

    def is_in_millions(self, column: str, pct: float = 1.0):
        self._check.is_in_millions(column=column, pct=pct)
        return self

    def is_in_billions(self, column: str, pct: float = 1.0):
        self._check.is_in_billions(column=column, pct=pct)
        return self

    def is_less_than(self, column: str, value: float, pct: float = 1.0):
        self._check.is_less_than(column=column, value=value, pct=pct)
        return self

    def is_negative(self, column: str, pct: float = 1.0):
        self._check.is_negative(column=column, pct=pct)
        return self

    def is_less_or_equal_than(self, column: str, value: float, pct: float = 1.0):
        self._check.is_less_or_equal_than(column=column, value=value, pct=pct)
        return self

    def is_equal_than(self, column: str, value: float, pct: float = 1.0):
        self._check.is_equal_than(column=column, value=value, pct=pct)
        return self

    def has_pattern(self, column: str, value: str, pct: float = 1.0):
        self._check.has_pattern(column=column, value=value, pct=pct)
        return self

    def is_legit(self, column: str, pct: float = 1.0):
        self._check.is_legit(column=column, pct=pct)
        return self

    def has_min(self, column: str, value: float):
        self._check.has_min(column=column, value=value)
        return self

    def has_max(self, column: str, value: float):
        self._check.has_max(column=column, value=value)
        return self

    def has_std(self, column: str, value: float):
        self._check.has_std(column=column, value=value)
        return self

    def has_mean(self, column: str, value: float):
        self._check.has_mean(column=column, value=value)
        return self

    def has_sum(self, column: str, value: float):
        self._check.has_sum(column=column, value=value)
        return self

    def is_between(self, column: str, value: List, pct: float = 1.0):
        self._check.is_between(column=column, value=tuple(value), pct=pct)
        return self

    def not_contained_in(self, column: str, value: List, pct: float = 1.0):
        self._check.not_contained_in(column=column, value=value, pct=pct)
        return self

    def not_in(self, column: str, value: List, pct: float = 1.0):
        self._check.not_in(column=column, value=tuple(value), pct=pct)
        return self

    def is_contained_in(self, column: str, value: Tuple | List, pct: float = 1.0):
        self._check.is_contained_in(column=column, value=value, pct=pct)
        return self

    def is_in(self, column: str, value: List, pct: float = 1.0):
        self._check.is_in(column=column, value=tuple(value), pct=pct)
        return self

    def is_t_minus_n(
            self,
            column: str,
            value: int,
            pct: float = 1.0,
            options: Dict[str, str] | None=None
    ):
        options = options or {}
        self._check.is_t_minus_n(column=column, value=value, pct=pct, options=options)
        return self

    def is_t_minus_1(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.is_t_minus_1(column=column, pct=pct)
        return self

    def is_t_minus_2(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.is_t_minus_2(column=column, pct=pct)
        return self

    def is_t_minus_3(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.is_t_minus_3(column=column, pct=pct)
        return self

    def is_yesterday(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.is_yesterday(column=column, pct=pct)
        return self

    def is_today(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.is_today(column=column, pct=pct)
        return self

    def has_percentile(
            self,
            column: str,
            value: float,
            percentile: float,
            precision: int = 10000,
    ):
        self._check.has_percentile(
            column=column,
            value=value,
            percentile=percentile,
            precision=precision
        )
        return self

    def is_inside_interquartile_range(
            self,
            column: str,
            value: List[float] | None = None,
            pct: float = 1.0
    ):
        self._check.is_inside_interquartile_range(column=column, value=value, pct=pct)
        return self

    def has_max_by(
            self,
            column_source: str,
            column_target: str,
            values: float | str
    ):
        self._check.has_max_by(
            column_source=column_source,
            column_target=column_target,
            value=values
        )
        return self

    def has_min_by(
            self,
            column_source: str,
            column_target: str,
            values: float | str
    ):
        self._check.has_min_by(
            column_source=column_source,
            column_target=column_target,
            value=values
        )
        return self

    def has_correlation(
            self,
            column_left: str,
            column_right: str,
            values: float,
    ):
        self._check.has_correlation(
            column_left=column_left,
            column_right=column_right,
            value=values
        )
        return self

    def satisfies(
            self,
            column: str,
            predicate: str,
            pct: float = 1.0,
            options: Dict[str, str] | None = None,
    ):
        options = options or {}
        self._check.satisfies(column=column, predicate=predicate, pct=pct, options=options)
        return self

    def has_cardinality(
            self,
            column: str,
            value: int,
    ):
        self._check.has_cardinality(column=column, value=value)
        return self

    def has_infogain(
            self,
            column: str,
            pct: float = 1.0,
    ):
        self._check.has_infogain(column=column, pct=pct)
        return self

    def has_entropy(
            self,
            column: str,
            value: float,
            tolerance: float = 0.01,
    ):
        self._check.has_entropy(column=column, value=value, tolerance=tolerance)
        return self

    def is_on_weekday(self, column: str, pct: float = 1.0):
        self._check.is_on_weekday(column=column, pct=pct)
        return self

    def is_on_weekend(self, column: str, pct: float = 1.0):
        self._check.is_on_weekend(column=column, pct=pct)
        return self

    def is_on_monday(self, column: str, pct: float = 1.0):
        self._check.is_on_monday(column=column, pct=pct)
        return self

    def is_on_tuesday(self, column: str, pct: float = 1.0):
        self._check.is_on_tuesday(column=column, pct=pct)
        return self

    def is_on_wednesday(self, column: str, pct: float = 1.0):
        self._check.is_on_wednesday(column=column, pct=pct)
        return self

    def is_on_thursday(self, column: str, pct: float = 1.0):
        self._check.is_on_thursday(column=column, pct=pct)
        return self

    def is_on_friday(self, column: str, pct: float = 1.0):
        self._check.is_on_friday(column=column, pct=pct)
        return self

    def is_on_saturday(self, column: str, pct: float = 1.0):
        self._check.is_on_saturday(column=column, pct=pct)
        return self

    def is_on_sunday(self, column: str, pct: float = 1.0):
        self._check.is_on_sunday(column=column, pct=pct)
        return self

    def is_on_schedule(self, column: str, value: List, pct: float = 1.0):
        self._check.is_on_schedule(column=column, value=tuple(value), pct=pct)
        return self

    def is_daily(
            self,
            column: str,
            value: List[int] | None = None,
            pct: float = 1.0,
    ):
        self._check.is_daily(column=column, value=value, pct=pct)
        return self

    def has_workflow(
            self,
            column_group: str,
            column_event: str,
            column_order: str,
            edges: List[Tuple[str]],
            pct: float = 1.0
    ):
        self._check.has_workflow(
            column_group=column_group,
            column_event=column_event,
            column_order=column_order,
            edges=edges,
            pct=pct
        )
        return self

    def is_custom(
            self,
            column: str | List[str],
            fn: Callable = None,
            pct: float = 1.0,
            options: Dict[str, str] | None = None
    ):
        options = options or {}
        self._check.is_custom(column=column, fn=fn, pct=pct, options=options)
        return self

    def _get_rule_metrics_pyspark(
        self,
        cuallee_result: Any,
    ) -> List[RuleMetric]:
        return [
            RuleMetric(
                id=row.id,
                rule=row.rule,
                column=row.column,
                value=row.value,
                rows=row.rows,
                violations=row.violations,
                pass_rate=row.pass_rate,
                pass_threshold=row.pass_threshold,
                options=self._check.rules[i].options,
                bad_records=None,
            )
            for i, row in enumerate(cuallee_result.collect())
        ]

    def _get_rule_metrics_pandas(
        self,
        cuallee_result: Any,
    ) -> List[RuleMetric]:
        return [
            RuleMetric(
                id=row["id"],
                rule=row["rule"],
                column=row["column"],
                value=row["value"],
                rows=row["rows"],
                violations=row["violations"],
                pass_rate=row["pass_rate"],
                pass_threshold=row["pass_threshold"],
                options=self._check.rules[i].options,
                bad_records=None,
            )
            for i, (index, row) in enumerate(cuallee_result.iterrows())
        ]

    def _to_check_result(
            self,
            cuallee_result: Any,
            start_time: datetime,
            end_time: datetime
    ) -> CheckResult:
        dtype = _get_df_type(cuallee_result)

        if "pyspark" in dtype:
            rule_metrics = self._get_rule_metrics_pyspark(cuallee_result=cuallee_result)
        elif "pandas" in dtype:
            rule_metrics = self._get_rule_metrics_pandas(cuallee_result=cuallee_result)
        else:
            raise ValueError(f"Unsupported dataframe type: {dtype}")

        return CheckResult(
            name=self.name,
            level=self.level,
            type=self.__class__.__name__,
            start_time=start_time,
            end_time=end_time,
            rule_metrics=rule_metrics,
        )

    def evaluate(self, df: Any) -> CheckResult:
        if "delta" in _get_df_type(df):
            df = df.toDF()

        start_time = datetime.now()
        result = self._check.validate(df)
        end_time = datetime.now()

        return self._to_check_result(
            cuallee_result=result,
            start_time=start_time,
            end_time=end_time
        )
