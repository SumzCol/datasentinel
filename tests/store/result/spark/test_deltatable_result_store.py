from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from ulid import ULID

from dataguard.store.result.core import ResultStoreError
from dataguard.store.result.spark.deltatable_result_store import DeltaTableResultStore
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric


@pytest.mark.unit
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
    def test_error_on_append(self, mock_delta_table_appender: Mock):
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
    @patch("dataguard.store.result.spark.deltatable_result_store.DeltaTableAppender")
    def test_store(self, mock_delta_table_appender: Mock):
        store = DeltaTableResultStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
        )

        data_validation_result = DataValidationResult(
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
                            pass_rate=0.5,
                            pass_threshold=1.0,
                            column=["col1", "col2"],
                        )
                    ],
                )
            ],
        )

        store.store(result=data_validation_result)

        mock_delta_table_appender.return_value.append.assert_called_once()
