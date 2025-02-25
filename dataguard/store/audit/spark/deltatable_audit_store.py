from datetime import date, datetime
from typing import Any, Literal

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from dataguard.store.audit.core import AbstractAuditStore, AuditStoreError
from dataguard.store.audit.row import BaseAuditRow, FieldInfo
from dataguard.store.utils.deltatable_appender import DeltaTableAppender


class DeltaTableAuditStore(AbstractAuditStore):
    def __init__(  # noqa PLR0913
        self,
        name: str,
        table: str,
        schema: str,
        dataset_type: Literal["file", "table"],
        external_path: str | None = None,
        save_args: dict[str, Any] | None = None,
        failed_rows_limit: int = 100,
        disabled: bool = False,
    ):
        super().__init__(name, disabled)
        if not failed_rows_limit > 0:
            raise AuditStoreError("Failed rows limit must be greater than 0")
        self._failed_rows_limit = failed_rows_limit
        self._delta_table_appender = DeltaTableAppender(
            table=table,
            schema=schema,
            dataset_type=dataset_type,
            external_path=external_path,
            save_args=save_args,
        )

    def append(self, row: BaseAuditRow) -> None:
        raise NotImplementedError

    def _infer_spark_type(self, field_info: FieldInfo) -> Any:
        if field_info.type in {list, tuple, set}:
            if not field_info.args and len(field_info.args) > 1:
                raise AuditStoreError("Multi-typed collections are not supported")

            return ArrayType(self._infer_spark_type(field_info.args[0]))

        type_map = {
            int: LongType,
            str: StringType,
            bool: BooleanType,
            float: DoubleType,
            datetime: TimestampType,
            date: DateType,
        }
        return type_map.get(field_info.type, StringType)
