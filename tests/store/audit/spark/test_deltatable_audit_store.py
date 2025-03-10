import json
from typing import Any
from unittest.mock import Mock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, BooleanType, DoubleType, LongType, StringType

from dataguard.store.audit.core import AuditStoreError
from dataguard.store.audit.row import BaseAuditRow
from dataguard.store.audit.spark.deltatable_audit_store import DeltaTableAuditStore


@pytest.fixture(scope="function")
def audit_row():
    def _create(field_type: type, field_value: Any) -> BaseAuditRow:
        class AuditRow(BaseAuditRow):
            field: field_type

        return AuditRow(field=field_value)

    return _create


@pytest.mark.unit
class TestDeltaTableAuditStoreUnit:
    @patch("dataguard.store.audit.spark.deltatable_audit_store.DeltaTableAppender")
    def test_successful_initialization(self, mock_delta_table_appender: Mock):
        store = DeltaTableAuditStore(
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

    @patch("dataguard.store.audit.spark.deltatable_audit_store.DeltaTableAppender")
    def test_error_on_invalid_failed_rows_limit_value(self, mock_delta_table_appender: Mock):
        with pytest.raises(AuditStoreError, match="Failed rows limit must be greater than 0"):
            DeltaTableAuditStore(
                name="test",
                table="test_table",
                schema="s3a://test_schema",
                dataset_type="file",
                include_failed_rows=True,
                failed_rows_limit=0,
            )
        mock_delta_table_appender.assert_not_called()

    @patch("dataguard.store.audit.spark.deltatable_audit_store.DeltaTableAppender")
    def test_error_on_append(self, mock_delta_table_appender: Mock, audit_row):
        store = DeltaTableAuditStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
        )

        mock_delta_table_appender.return_value.append.side_effect = Exception("Mock exception")
        audit_row_mock = Mock(spec=BaseAuditRow)

        with patch.object(store, "_audit_row_to_df") as mock_audit_row_to_df:
            with pytest.raises(
                AuditStoreError,
                match="Failed to append row to audit store. Error: Mock exception",
            ):
                store.append(audit_row_mock)

            mock_audit_row_to_df.assert_called_once()

    @pytest.mark.slow
    @pytest.mark.pyspark
    @pytest.mark.parametrize(
        "field_type, field_value, expected_spark_type, expected_value",
        [
            # Primitive types
            (int, 1, LongType(), 1),
            (str, "test", StringType(), "test"),
            (float, 1.0, DoubleType(), 1.0),
            (bool, True, BooleanType(), True),
            # Collection types
            (list[int], [1, 2], ArrayType(LongType()), [1, 2]),
            (list[str], ["test1", "test2"], ArrayType(StringType()), ["test1", "test2"]),
            (list[float], [1.0, 2.0], ArrayType(DoubleType()), [1.0, 2.0]),
            (list[bool], [True, False], ArrayType(BooleanType()), [True, False]),
            (tuple[int, ...], (1, 2), ArrayType(LongType()), [1, 2]),
            (tuple[str, ...], ("test1", "test2"), ArrayType(StringType()), ["test1", "test2"]),
            (tuple[float, ...], (1.0, 2.0), ArrayType(DoubleType()), [1.0, 2.0]),
            (tuple[bool, ...], (True, False), ArrayType(BooleanType()), [True, False]),
            (set[int], {1, 2}, ArrayType(LongType()), [1, 2]),
            (set[str], {"test1", "test2"}, ArrayType(StringType()), ["test1", "test2"]),
            (set[float], {1.0, 2.0}, ArrayType(DoubleType()), [1.0, 2.0]),
            (set[bool], {True, False}, ArrayType(BooleanType()), [True, False]),
            (dict[str, Any], {"test1": 1}, StringType(), json.dumps({"test1": 1})),
        ],
    )
    @patch("dataguard.store.audit.spark.deltatable_audit_store.DeltaTableAppender")
    def test_audit_row_to_df(
        self,
        mock_delta_table_appender: Mock,
        field_type: type,
        field_value: Any,
        expected_spark_type: Any,
        expected_value: Any,
        audit_row,
        spark: SparkSession,
    ):
        store = DeltaTableAuditStore(
            name="test",
            table="test_table",
            schema="test_schema",
            dataset_type="file",
            external_path="s3a://test_path",
        )

        audit_row = audit_row(field_type=field_type, field_value=field_value)

        with patch("dataguard.store.utils.spark_utils.get_spark", return_value=spark):
            store.append(audit_row)

        result_df = mock_delta_table_appender.return_value.append.call_args.args[0]
        row = result_df.collect()[0]

        assert result_df.count() == 1

        # As sets are converted to list, they can end up in different order, so we need to compare
        # them as sets
        if audit_row.row_fields["field"].type is set:
            assert set(row[0]) == set(expected_value)
        else:
            assert row[0] == expected_value
        assert result_df.schema[0].dataType == expected_spark_type
