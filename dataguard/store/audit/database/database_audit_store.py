import json
from datetime import date, datetime
from typing import Any, Literal

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.exc import (
    NoSuchTableError,
    SQLAlchemyError,
)
from sqlalchemy.orm import sessionmaker

from dataguard.store.audit.core import AbstractAuditStore, AuditStoreError
from dataguard.store.audit.row import BaseAuditRow


class DatabaseAuditStore(AbstractAuditStore):
    """Audit store implementation that appends audit data to a database using SQLAlchemy."""

    def __init__(
        self,
        name: str,
        disabled: bool,
        table: str,
        schema: str,
        credentials: dict[str, Any],
        if_table_not_exists: Literal["create", "error"] = "error",
    ):
        if "connection_string" not in credentials:
            raise AuditStoreError("Connection string not found in credentials.")
        connection_string = credentials["connection_string"]

        self._engine = create_engine(connection_string)
        self._table = table
        self._schema = schema
        self._if_table_not_exists = if_table_not_exists
        self._metadata = MetaData()
        self._session_maker = sessionmaker(bind=self._engine)
        super().__init__(name, disabled)

    def _get_or_create_table(self, row: BaseAuditRow) -> Table:
        try:
            return Table(
                self._table,
                self._metadata,
                schema=self._schema,
                autoload_with=self._engine,
            )
        except NoSuchTableError as e:
            if not self._if_table_not_exists == "create":
                raise AuditStoreError(
                    f"Table '{self._table}' does not exist and its configured to not be created "
                    "or its not accessible."
                ) from e
        except SQLAlchemyError as e:
            raise AuditStoreError(
                f"There was an error while trying to get or create table '{self._table}'. "
                f"Error: {e!s}"
            ) from e

        return self._create_table(row)

    def _create_table(self, row: BaseAuditRow) -> Table:
        try:
            columns = []
            for name, info in row.row_fields.items():
                column_type = self._infer_sql_type(info.type)
                columns.append(Column(name, column_type))

            table = Table(self._table, self._metadata, schema=self._schema, *columns)
            self._metadata.create_all(self._engine)

            return table
        except SQLAlchemyError as e:
            raise AuditStoreError(
                f"There was an error while trying to create table '{self._table}'. Error: {e!s}"
            ) from e

    def append(self, row: BaseAuditRow):
        table = self._get_or_create_table(row)
        with self._session_maker() as session:
            try:
                row_dict = {
                    field: self._serialize_field_value(value)
                    for field, value in row.to_dict().items()
                }
                insert_statement = table.insert().values(row_dict)
                session.execute(insert_statement)
                session.commit()
            except Exception as e:
                session.rollback()
                raise AuditStoreError(
                    f"There was an error while trying to append row. Error: {e!s}"
                ) from e

    @staticmethod
    def _serialize_field_value(value: Any) -> Any:
        """Serialize a field value."""
        if isinstance(value, int | str | float | bool):
            return value
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.isoformat()
        elif value is None:
            return value
        else:
            return json.dumps(value)

    @staticmethod
    def _infer_sql_type(
        python_type: type[int | str | bool | float | datetime | date],
    ) -> type[Integer | String | Boolean | Float | DateTime | Date]:
        """Infer the SQLAlchemy column type based on the Python data type."""
        type_map = {
            int: Integer,
            str: String,
            bool: Boolean,
            float: Float,
            datetime: DateTime,
            date: Date,
        }

        return type_map.get(python_type, String)
