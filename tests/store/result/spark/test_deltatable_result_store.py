from datetime import datetime
import json
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
import pytest
from ulid import ULID

from dataguard.store.result.core import ResultStoreError
from dataguard.store.result.spark.deltatable_result_store import DeltaTableResultStore
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.failed_rows_dataset.spark import SparkFailedRowsDataset
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric


@pytest.fixture(scope="function")
def data_validation_result(spark: SparkSession) -> DataValidationResult:
    return DataValidationResult(
        run_id=ULID(),
        name="test",
        data_asset="test_asset",
        data_asset_schema="test_schema",
        start_time=datetime.now(),
        end_time=datetime.now(),
        check_results=[
            CheckResult(
                name="test_check",
                level=CheckLevel.ERROR,
                class_name="TestCheck",
                start_time=datetime.now(),
                end_time=datetime.now(),
                rule_metrics=[
                    RuleMetric(
                        id=1,
                        rule="test_rule",
                        rows=10,
                        violations=5,
                        function=lambda x: x,
                        pass_rate=0.5,
                        pass_threshold=1.0,
                        column=["col1", "col2"],
                        options={"key": "value"},
                        failed_rows_dataset=(
                            SparkFailedRowsDataset(
                                spark.createDataFrame(
                                    data=[(1, "a"), (2, "b")],
                                    schema=["col1", "col2"],
                                )
                            )
                        ),
                    )
                ],
            )
        ],
    )


@pytest.mark.unit
@pytest.mark.result_store
class TestDeltaTableResultStoreUnit:
    @patch("dataguard.store.result.spark.deltatable_result_store.DeltaTableAppender")
    def test_successful_initialization(self, mock_delta_table_appender: Mock):
        store = DeltaTableResultStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
            save_args={"mode": "append"},
            include_failed_rows=True,
            failed_rows_limit=100,
            disabled=True,
        )

        assert store.name == "test"
        assert store.disabled
        mock_delta_table_appender.assert_called_once_with(
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
            save_args={"mode": "append"},
        )

    @patch("dataguard.store.result.spark.deltatable_result_store.DeltaTableAppender")
    def test_error_on_invalid_failed_rows_limit_value(self, mock_delta_table_appender: Mock):
        with pytest.raises(ResultStoreError, match="Failed rows limit must be greater than 0"):
            DeltaTableResultStore(
                name="test",
                table="test_table",
                schema="s3a://test_schema",
                dataset_type="file",
                include_failed_rows=True,
                failed_rows_limit=0,
            )
        mock_delta_table_appender.assert_not_called()

    @patch("dataguard.store.result.spark.deltatable_result_store.DeltaTableAppender")
    def test_error_on_store(self, mock_delta_table_appender: Mock):
        store = DeltaTableResultStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
        )

        mock_delta_table_appender.return_value.append.side_effect = Exception("Mock exception")
        data_validation_result_mock = Mock(spec=DataValidationResult)

        with patch.object(store, "_result_to_df") as mock_result_to_df:
            with pytest.raises(
                ResultStoreError,
                match="Error while saving data validation result in delta table: Mock exception",
            ):
                store.store(data_validation_result_mock)

            mock_result_to_df.assert_called_once()

    @pytest.mark.slow
    @pytest.mark.pyspark
    @pytest.mark.parametrize(
        "include_failed_rows",
        [
            True,
            False,
        ],
    )
    @patch("dataguard.store.result.spark.deltatable_result_store.DeltaTableAppender")
    def test_result_df(
        self,
        mock_delta_table_appender: Mock,
        include_failed_rows: bool,
        spark: SparkSession,
        data_validation_result: DataValidationResult,
    ):
        failed_rows_limit = 1
        store = DeltaTableResultStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
            include_failed_rows=include_failed_rows,
            failed_rows_limit=failed_rows_limit,
        )
        with patch("dataguard.store.utils.spark_utils.get_spark", return_value=spark):
            store.store(result=data_validation_result)
        result_df = mock_delta_table_appender.return_value.append.call_args.args[0]
        row = result_df.collect()[0]

        assert result_df.count() == 1
        assert row.run_id == str(data_validation_result.run_id)
        assert row.rule_function == RuleMetric.function_to_string(
            data_validation_result.check_results[0].rule_metrics[0].function
        )
        assert row.rule_options == json.dumps(
            data_validation_result.check_results[0].rule_metrics[0].options
        )
        assert row.rule_failed_rows_dataset == (
            data_validation_result.check_results[0]
            .rule_metrics[0]
            .failed_rows_dataset.to_json(limit=failed_rows_limit)
            if include_failed_rows
            else None
        )
