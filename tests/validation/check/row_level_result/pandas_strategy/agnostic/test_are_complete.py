from datetime import date, datetime

import pytest
from pandas import DataFrame

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
class TestAreCompleteUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [(1, 1, 1), (2, 2, 2)],
            [(1, "a", "a"), (2, "b", "b")],
            [(1, 1.0, 1.0), (2, 2.0, 2.0)],
            [(1, date.today(), date.today()), (2, date.today(), date.today())],
            [(1, datetime.now(), datetime.now()), (2, datetime.now(), datetime.now())],
        ],
    )
    def test_pass(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        id_columns = ["id"]
        df = DataFrame(data=data, columns=[*id_columns, *evaluated_columns])
        evaluated_rows = len(data)
        expected_violations = 0

        result = check.are_complete(id_columns=id_columns, column=evaluated_columns).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.to_dict() == []

    @pytest.mark.parametrize(
        "data",
        [
            [(1, 2, None), (2, None, 3), (3, None, None)],
            [(1, "b", None), (2, None, "c"), (3, None, None)],
            [(1, 2.0, None), (2, None, 3.0), (3, None, None)],
            [(1, date.today(), None), (2, None, date.today()), (3, None, None)],
            [(1, datetime.now(), None), (2, None, datetime.now()), (3, None, None)],
        ],
    )
    def test_fail(self, check: RowLevelResultCheck, data: list[tuple]):
        evaluated_columns = ["col1", "col2"]
        id_columns = ["id"]
        df = DataFrame(data=data, columns=[*id_columns, *evaluated_columns])
        evaluated_rows = len(data)
        expected_violations = 3

        result = check.are_complete(id_columns=id_columns, column=evaluated_columns).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == [
            *id_columns,
            *evaluated_columns,
        ]
