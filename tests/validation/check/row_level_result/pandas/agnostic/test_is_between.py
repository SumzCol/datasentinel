from typing import Any

from pandas import DataFrame
import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status
from tests.validation.check.row_level_result.utils.agnostic_check_tests_cases import (
    is_between_tests_cases_parameterize,
)


@pytest.mark.unit
@pytest.mark.pandas
class TestIsBetweenUnit:
    @pytest.mark.parametrize(
        **is_between_tests_cases_parameterize(pass_outcome=True),
    )
    def test_pass(
        self,
        check: RowLevelResultCheck,
        data: list[tuple],
        columns: list[str],
        evaluated_column: str,
        min_value: Any,
        max_value: Any,
    ):
        evaluated_rows = len(data)

        df = DataFrame(data=data, columns=columns)
        result = check.is_between(
            column=evaluated_column, lower_bound=min_value, upper_bound=max_value
        ).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == 0
        assert result.rule_metrics[0].failed_rows_dataset is None
        assert result.rule_metrics[0].column == [evaluated_column]

    @pytest.mark.parametrize(
        **is_between_tests_cases_parameterize(pass_outcome=False),
    )
    def test_fail(
        self,
        check: RowLevelResultCheck,
        data: list[tuple],
        columns: list[str],
        evaluated_column: str,
        id_columns: list[str],
        min_value: Any,
        max_value: Any,
        expected_violations: int,
    ):
        evaluated_rows = len(data)

        df = DataFrame(data=data, columns=columns)
        result = check.is_between(
            column=evaluated_column,
            id_columns=id_columns,
            lower_bound=min_value,
            upper_bound=max_value,
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == columns
        assert result.rule_metrics[0].column == [evaluated_column]
        assert result.rule_metrics[0].id_columns == id_columns
