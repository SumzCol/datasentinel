from datetime import date, datetime

import pytest
from pandas import DataFrame

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestAreUniqueUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (2, 2)],
            [("a", "a"), ("b", "b")],
            [(1.0, 1.0), (2.0, 2.0)],
            [(date(2020, 1, 1), date(2020, 1, 1)), (date(2020, 1, 2), date(2020, 1, 2))],
            [
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                (datetime(2020, 1, 2), datetime(2020, 1, 2)),
            ],
        ],
    )
    def test_pass(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 0

        df = DataFrame(data, columns=evaluated_columns)
        result = check.are_unique(column=evaluated_columns).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (1, 1)],
            [("a", "a"), ("a", "a")],
            [(1.0, 1.0), (1.0, 1.0)],
            [(date(2020, 1, 1), date(2020, 1, 1)), (date(2020, 1, 1), date(2020, 1, 1))],
            [
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
                (datetime(2020, 1, 1), datetime(2020, 1, 1)),
            ],
        ],
    )
    def test_fail(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 1

        df = DataFrame(data, columns=evaluated_columns)
        result = check.are_unique(column=evaluated_columns).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == evaluated_columns

    @pytest.mark.parametrize(
        "ignore_nulls, result_status",
        [
            (False, Status.FAIL),
            (True, Status.PASS),
        ],
    )
    def test_with_ignoring_nulls_param(
        self,
        check: RowLevelResultCheck,
        ignore_nulls: bool,
        result_status: Status,
    ):
        data = [(1, 1), (1, None), (1, None), (None, 1), (None, 1), (None, None), (None, None)]
        evaluated_columns = ["col1", "col2"]

        df = DataFrame(data=data, columns=evaluated_columns)
        result = check.are_unique(column=evaluated_columns, ignore_nulls=ignore_nulls).validate(
            df
        )

        assert result.status == result_status
