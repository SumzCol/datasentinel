from pandas import DataFrame
import pytest

from dataguard.validation.failed_rows_dataset.core import FailedRowsDatasetError
from dataguard.validation.failed_rows_dataset.pandas import PandasFailedRowsDataset


@pytest.fixture(scope="function")
def pandas_df():
    return DataFrame(
        {
            "id": [1, 2],
            "name": ["a", "b"],
        }
    )


@pytest.mark.unit
class TestPandasFailedRowsDatasetUnit:
    def test_data_property(self, pandas_df: DataFrame):
        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        assert failed_rows_dataset.data.equals(pandas_df)

    def test_count(self, pandas_df: DataFrame):
        expected_count = pandas_df.shape[0]
        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        assert failed_rows_dataset.count() == expected_count

    @pytest.mark.parametrize(
        "limit",
        [None, 1, 2, 3],
    )
    def test_to_dict(self, limit: int | None, pandas_df: DataFrame):
        expected_result = (
            pandas_df.head(limit).to_dict(orient="records")
            if limit is not None
            else pandas_df.to_dict(orient="records")
        )

        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        assert failed_rows_dataset.to_dict(limit=limit) == expected_result

    @pytest.mark.parametrize(
        "limit",
        [-1, 0],
    )
    def test_error_to_dict_with_less_than_zero_limit(self, limit: int, pandas_df: DataFrame):
        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        with pytest.raises(FailedRowsDatasetError, match="Limit must be greater than 0"):
            failed_rows_dataset.to_dict(limit=limit)

    @pytest.mark.parametrize(
        "limit",
        [None, 1, 2, 3],
    )
    def test_to_json(self, limit: int | None, pandas_df: DataFrame):
        expected_result = (
            pandas_df.head(limit).to_json(orient="records")
            if limit is not None
            else pandas_df.to_json(orient="records")
        )
        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        assert failed_rows_dataset.to_json(limit=limit) == expected_result

    @pytest.mark.parametrize(
        "limit",
        [-1, 0],
    )
    def test_error_to_json_with_less_than_zero_limit(self, limit: int, pandas_df: DataFrame):
        failed_rows_dataset = PandasFailedRowsDataset(data=pandas_df)

        with pytest.raises(FailedRowsDatasetError, match="Limit must be greater than 0"):
            failed_rows_dataset.to_json(limit=limit)
