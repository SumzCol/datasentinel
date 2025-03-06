from datetime import date, datetime

import pytest
from pandas import DataFrame

from dataguard.validation.check import CualleeCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestIsBetweenUnit:
    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("a",)], ["a", "b"]),
            # Integer
            ([(1,)], [1, 2]),
            # Float
            ([(1.0,)], [1.0, 2.0]),
            # Date
            ([(date(2020, 1, 1),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            ([(datetime(2020, 1, 1),)], [datetime(2020, 1, 1), datetime(2020, 1, 2)]),
        ],
    )
    def test_pass(self, check: CualleeCheck, data: list[tuple], value: list) -> None:
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_between(column=evaluated_column, value=value).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        "data, value",
        [
            # String
            ([("c",)], ["a", "b"]),
            # Int
            ([(3,)], [1, 2]),
            # Float
            ([(2.0000001,)], [1.0, 2.0]),
            # Date
            ([(date(2019, 12, 31),)], [date(2020, 1, 1), date(2020, 1, 2)]),
            # Timestamp
            (
                [(datetime(2020, 12, 31),)],
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

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_between(
            column=evaluated_column,
            value=value,
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].column == [evaluated_column]
