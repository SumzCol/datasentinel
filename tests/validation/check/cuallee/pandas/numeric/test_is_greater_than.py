from pandas import DataFrame
import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.status import Status


@pytest.mark.unit
class TestIsGreaterThanUnit:
    def test_pass(self, check: CualleeCheck):
        data = [(1.00000000001,), (2.0,)]
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"
        df = DataFrame(data=data, columns=[evaluated_column])

        result = check.is_greater_than(column=evaluated_column, value=1.0).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        "data",
        [
            [(0.9999999999,)],
            [(1.0,)],
            [(0.98,)],
        ],
    )
    def test_fail(
        self,
        check: CualleeCheck,
        data: list[tuple],
    ):
        evaluated_rows = len(data)
        evaluated_column = "col"
        expected_violations = 1
        df = DataFrame(data=data, columns=[evaluated_column])

        result = check.is_greater_than(column=evaluated_column, value=1.0).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
