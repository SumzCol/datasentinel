from pandas import DataFrame
import pytest

from datasentinel.validation.check import RowLevelResultCheck
from datasentinel.validation.status import Status
from tests.validation.check.row_level_result.utils.agnostic_check_tests_cases import (
    are_unique_tests_cases_parameterize,
)


@pytest.mark.unit
@pytest.mark.pandas
class TestAreUniqueUnit:
    @pytest.mark.parametrize(
        **are_unique_tests_cases_parameterize(pass_outcome=True),
    )
    def test_pass(
        self,
        check: RowLevelResultCheck,
        data: list[tuple],
        columns: list[str],
        evaluated_column: list[str],
        ignore_nulls: bool,
    ):
        evaluated_rows = len(data)

        df = DataFrame(data=data, columns=columns)
        result = check.are_unique(column=evaluated_column, ignore_nulls=ignore_nulls).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == 0
        assert result.rule_metrics[0].failed_rows_dataset is None
        assert result.rule_metrics[0].column == evaluated_column

    @pytest.mark.parametrize(
        **are_unique_tests_cases_parameterize(pass_outcome=False),
    )
    def test_fail(
        self,
        check: RowLevelResultCheck,
        data: list[tuple],
        columns: list[str],
        evaluated_column: list[str],
        ignore_nulls: bool,
        expected_violations: int,
    ):
        evaluated_rows = len(data)

        df = DataFrame(data=data, columns=columns)
        result = check.are_unique(column=evaluated_column, ignore_nulls=ignore_nulls).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == columns
        assert result.rule_metrics[0].column == evaluated_column
