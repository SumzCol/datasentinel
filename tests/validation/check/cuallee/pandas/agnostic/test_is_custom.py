from pandas import DataFrame
import pytest

from dataguard.validation.check import CualleeCheck
from dataguard.validation.status import Status


@pytest.fixture(scope="function")
def pandas_df():
    return DataFrame({"col": ["b", "c"]})


@pytest.mark.unit
@pytest.mark.pandas
class TestIsCustomUnit:
    def test_pass(self, check: CualleeCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe[dataframe[evaluated_column].isin(["b", "c"])]

        expected_rows = pandas_df.shape[0]
        expected_violations = 0

        result = check.is_custom(fn=fn, column=evaluated_column).validate(pandas_df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].function is not None
        assert result.rule_metrics[0].value is None
        assert result.rule_metrics[0].column == [evaluated_column]

    def test_fail(self, check: CualleeCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe[dataframe[evaluated_column] == "b"]

        expected_rows = pandas_df.shape[0]
        expected_violations = 1

        result = check.is_custom(fn=fn, column=evaluated_column).validate(pandas_df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].function is not None
        assert result.rule_metrics[0].value is None
        assert result.rule_metrics[0].column == [evaluated_column]
