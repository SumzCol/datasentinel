from datetime import date, datetime, timedelta

import pytest
from pandas import DataFrame

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
class TestIsUniqueUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1,), (2,)],
            [("a",), ("b",)],
            [(1.0,), (2.0,)],
            [(date.today(),), (date.today() + timedelta(days=10),)],
            [(datetime.now(),), (datetime.now() + timedelta(days=10),)],
        ],
    )
    def test_pass(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_column = "col"
        df = DataFrame(data=data, columns=[evaluated_column])
        evaluated_rows = len(data)
        expected_violations = 0

        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.to_dict() == []

    @pytest.mark.parametrize(
        "data",
        [
            [(1,), (1,)],
            [("a",), ("a",)],
            [(1.0,), (1.0,)],
            [(date(2020, 1, 1),), (date(2020, 1, 1),)],
            [(datetime(2020, 1, 1),), (datetime(2020, 1, 1),)],
        ],
    )
    def test_fail(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_column = "col"
        df = DataFrame(data=data, columns=[evaluated_column])
        evaluated_rows = len(data)
        expected_violations = 1

        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == [evaluated_column]

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
        data = [(1,), (None,), (None,)]
        df = DataFrame(data=data, columns=["id"])
        result = check.is_unique(column="id", ignore_nulls=ignore_nulls).validate(df)
        assert result.status == result_status
