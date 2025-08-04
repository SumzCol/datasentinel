from datetime import date, datetime

from pandas import DataFrame
import pytest

from dataguard.validation.check import CualleeCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestIsInUnit:
    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("c",)], ["a", "b"]),
            # Integer
            ([(3,)], [1, 2]),
            # Float
            ([(3.0,)], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 3),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            ([(datetime(2020, 1, 3),)], [datetime(2020, 1, 1), datetime(2020, 1, 2)]),
        ],
    )
    def test_pass(self, check: CualleeCheck, data: list[tuple], value: list) -> None:
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = DataFrame(data, columns=[evaluated_column])
        result = check.not_in(column=evaluated_column, value=value).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("a",)], ["a", "b"]),
            # Int
            ([(1,)], [1, 2]),
            # Float
            ([(1.0,)], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 2),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            (
                [(datetime(2020, 1, 2),)],
                [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            ),
        ],
    )
    def test_fail(
        self,
        check: CualleeCheck,
        data: list[tuple],
        value: list,
    ) -> None:
        evaluated_rows = len(data)
        expected_violations = 1
        evaluated_column = "col"

        df = DataFrame(data, columns=[evaluated_column])
        result = check.not_in(column=evaluated_column, value=value).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
