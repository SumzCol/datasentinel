import pytest
from pandas import DataFrame

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.pandas
class TestIsEqualToUnit:
    def test_pass(self, check: RowLevelResultCheck):
        data = [
            (1.0,),
        ]
        evaluated_rows = len(data)
        expected_violations = 0
        evaluated_column = "col"

        df = DataFrame(data=data, columns=[evaluated_column])
        result = check.is_equal_to(column=evaluated_column, value=1.0).validate(df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    @pytest.mark.parametrize(
        "data, schema, id_columns",
        [
            ([(1.0000000000001,)], ["col"], []),
            ([(1.0000000000001,)], ["col"], ["col"]),
            ([(1, 2.0)], ["id", "col"], ["id"]),
        ],
    )
    def test_fail_with_and_without_id_columns(
        self,
        check: RowLevelResultCheck,
        data: list[tuple],
        schema: list[str],
        id_columns: list[str],
    ):
        evaluated_rows = len(data)
        evaluated_column = "col"
        expected_violations = 1

        df = DataFrame(data=data, columns=schema)
        result = check.is_equal_to(
            column=evaluated_column, value=1.0, id_columns=id_columns
        ).validate(df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == evaluated_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert list(result.rule_metrics[0].failed_rows_dataset.data.columns) == schema
