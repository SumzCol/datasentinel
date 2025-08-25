from datetime import date, datetime

from pandas import DataFrame
import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestIsCompleteUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1,), (2,)],
            [("a",), ("b",)],
            [(1.0,), (2.0,)],
            [(date.today()), (date.today())],
            [(datetime.now(),), (datetime.now(),)],
        ],
    )
    def test_pass(self, check: CualleeCheck, data: list[tuple]):
        evaluated_column = "col"
        evaluated_rows = len(data)
        expected_violations = 0

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_complete(column=evaluated_column).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        "data",
        [
            [
                (1,),
                (None,),
            ],
            [
                ("a",),
                (None,),
            ],
            [
                (1.02,),
                (None,),
            ],
            [
                (date.today(),),
                (None,),
            ],
            [
                (datetime.now(),),
                (None,),
            ],
        ],
    )
    def test_fail(self, check: CualleeCheck, data: list[tuple]):
        evaluated_column = "col"
        evaluated_rows = len(data)
        expected_violations = 1

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_complete(column=evaluated_column).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
