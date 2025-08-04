from datetime import date, datetime

from pandas import DataFrame
import pytest

from dataguard.validation.check import CualleeCheck
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
    def test_pass(self, check: CualleeCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 0

        df = DataFrame(data, columns=evaluated_columns)
        result = check.are_unique(column=evaluated_columns).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == evaluated_columns

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
    def test_fail(self, check: CualleeCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        evaluated_rows = len(data)
        expected_violations = 1

        df = DataFrame(data, columns=evaluated_columns)
        result = check.are_unique(column=evaluated_columns).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == evaluated_columns
