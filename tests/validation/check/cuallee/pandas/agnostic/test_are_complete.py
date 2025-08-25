from datetime import date, datetime

from pandas import DataFrame
import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestAreCompleteUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1), (2, 2)],
            [("a", "a"), ("b", "b")],
            [(1.0, 1.0), (2.0, 2.0)],
            [(date.today(), date.today()), (date.today(), date.today())],
            [(datetime.now(), datetime.now()), (datetime.now(), datetime.now())],
        ],
    )
    def test_pass(self, check: CualleeCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 0

        df = DataFrame(data=data, columns=[*evaluated_columns])
        result = check.are_complete(column=evaluated_columns).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == evaluated_columns

    @pytest.mark.parametrize(
        "data",
        [
            [(2, None), (None, 3), (None, None)],
            [("b", None), (None, "c"), (None, None)],
            [(2.0, None), (None, 3.0), (None, None)],
            [(date.today(), None), (None, date.today()), (None, None)],
            [(datetime.now(), None), (None, datetime.now()), (None, None)],
        ],
    )
    def test_fail(self, check: CualleeCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 2

        df = DataFrame(data=data, columns=[*evaluated_columns])
        result = check.are_complete(column=evaluated_columns).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == evaluated_columns
