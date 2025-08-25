from pandas import DataFrame
import pytest

from datasentinel.validation.check import RowLevelResultCheck
from datasentinel.validation.status import Status


@pytest.fixture(scope="function")
def pandas_df():
    return DataFrame({"col": ["b", "c"]})


@pytest.mark.unit
@pytest.mark.pandas
class TestIsCustomUnit:
    def test_pass_without_options(self, check: RowLevelResultCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe[dataframe[evaluated_column] == "a"]

        expected_rows = pandas_df.shape[0]
        expected_violations = 0

        result = check.is_custom(fn).validate(pandas_df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    def test_pass_with_options(self, check: RowLevelResultCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame, options: dict) -> DataFrame:
            return dataframe[dataframe[evaluated_column] == options["value"]]

        expected_rows = pandas_df.shape[0]
        expected_violations = 0

        result = check.is_custom(fn=fn, options={"value": "a"}).validate(pandas_df)

        assert result.status == Status.PASS
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset is None

    def test_fail_without_options(self, check: RowLevelResultCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame) -> DataFrame:
            return dataframe[dataframe[evaluated_column] == "b"]

        expected_rows = pandas_df.shape[0]
        expected_violations = 1

        result = check.is_custom(fn).validate(pandas_df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [evaluated_column]

    def test_fail_with_options(self, check: RowLevelResultCheck, pandas_df: DataFrame):
        evaluated_column = "col"

        def fn(dataframe: DataFrame, options: dict) -> DataFrame:
            return dataframe[dataframe[evaluated_column] == options["value"]]

        expected_rows = pandas_df.shape[0]
        expected_violations = 1

        result = check.is_custom(fn=fn, options={"value": "b"}).validate(pandas_df)

        assert result.status == Status.FAIL
        assert result.rule_metrics[0].rows == expected_rows
        assert result.rule_metrics[0].violations == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.count() == expected_violations
        assert result.rule_metrics[0].failed_rows_dataset.data.columns == [evaluated_column]

    def test_error_on_fn_that_does_not_return_a_pandas_dataframe(
        self, check: RowLevelResultCheck, pandas_df: DataFrame
    ):
        def fn(dataframe: DataFrame) -> bool:
            return True

        with pytest.raises(ValueError):
            check.is_custom(fn=fn).validate(pandas_df)
