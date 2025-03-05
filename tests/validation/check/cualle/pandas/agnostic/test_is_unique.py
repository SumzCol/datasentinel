from datetime import date, datetime, timedelta

import pytest
from pandas import DataFrame

from dataguard.validation.check import CualleeCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
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
    def test_pass(self, check: CualleeCheck, data: list[tuple]):
        evaluated_column = "col"
        evaluated_rows = len(data)
        expected_violations = 0

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

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
    def test_fail(self, check: CualleeCheck, data: list[tuple]):
        evaluated_column = "col"
        evaluated_rows = len(data)
        expected_violations = 1

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_unique(column=evaluated_column).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
