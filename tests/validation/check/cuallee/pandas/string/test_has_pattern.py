import pytest
from pandas import DataFrame

from dataguard.validation.check import CualleeCheck
from dataguard.validation.status import Status


@pytest.mark.unit
class TestHasPatternUnit:
    def test_pass(self, check: CualleeCheck):
        data = [("spark",)]
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"
        df = DataFrame(data=data, columns=[evaluated_column])

        result = check.has_pattern(column=evaluated_column, value=r"^s.+k$").validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        "data",
        [
            [("pandas",)],
            [("pyspark",)],
            [("example",)],
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

        result = check.has_pattern(column=evaluated_column, value=r"^s.+k$").validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
