import json
from typing import Any, Literal, Dict

from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Boolean, MetaData
from sqlalchemy.exc import (
    NoSuchTableError,
    SQLAlchemyError,
)
from sqlalchemy.orm import sessionmaker

from dataguard.store.audit.core import AbstractAuditStore, AuditStoreError
from dataguard.store.audit.row import AuditRow


def _infer_sqlalchemy_type(value: Any) -> Any:
    """Infer the SQLAlchemy column type based on the Python data type."""
    if isinstance(value, int):
        return Integer
    elif isinstance(value, float):
        return Float
    elif isinstance(value, bool):
        return Boolean
    elif isinstance(value, (list, set, tuple, dict)):
        return String
    else:
        return String


def _serialize_field_value(value: Any) -> Any:
    """Seralize a field value."""
    if isinstance(value, (int, float, bool, str)): return value
    if isinstance(value, (list, set, tuple, dict)):
        return json.dumps(value if not isinstance(value, set) else list(value))

    raise AuditStoreError(f"Cannot serialize value of type {type(value)}")


class DatabaseAuditStore(AbstractAuditStore):
    """Audit store implementation that appends to a database using SQLAlchemy."""

    def __init__(
        self,
        name: str,
        disabled: bool,
        table: str,
        schema: str,
        credentials: Dict[str, Any],
        if_not_exists: Literal["create", "error"] = "error",
    ):
        if "connection_string" not in credentials:
            raise AuditStoreError("Connection string not found in credentials.")
        connection_string = credentials["connection_string"]

        self._engine = create_engine(connection_string)
        self._table = table
        self._schema = schema
        self._if_not_exists = if_not_exists
        self._metadata = MetaData()
        self._session_maker = sessionmaker(bind=self._engine)
        super().__init__(name, disabled)

    def _get_or_create_table(self, row_dict: Dict[str, Any]):
        try:
            table = Table(
                self._table,
                self._metadata,
                schema=self._schema,
                autoload_with=self._engine
            )

            return table
        except NoSuchTableError as e:
            if not self._if_not_exists == "create":
                raise AuditStoreError(
                    f"Table '{self._table}' does not exist (its configured to not be created) "
                    "or its not accessible."
                ) from e

            return self._create_table(row_dict)
        except SQLAlchemyError as e:
            raise AuditStoreError(
                f"There was an error while trying to get or create table '{self._table}'. "
                f"Error: {str(e)}"
            ) from e

    def _create_table(self, row_dict: Dict[str, Any]) -> Table:
        try:
            columns = []
            for name, value in row_dict.items():
                column_type = _infer_sqlalchemy_type(value)
                columns.append(Column(name, column_type))

            table = Table(
                self._table,
                self._metadata,
                schema=self._schema,
                *columns
            )
            self._metadata.create_all(self._engine)

            return table
        except SQLAlchemyError as e:
            raise AuditStoreError(
                f"There was an error while trying to create table '{self._table}'. "
                f"Error: {str(e)}"
            ) from e

    def append(self, row: AuditRow):
        """Append a row to the audit store."""
        row_dict = {
            field: _serialize_field_value(value)
            for field, value in row.to_dict().items()
        }
        table = self._get_or_create_table(row_dict)
        with self._session_maker() as session:
            try:
                insert_statement = table.insert().values(row_dict)
                session.execute(insert_statement)
                session.commit()
            except Exception as e:
                session.rollback()
                raise AuditStoreError(
                    f"There was an error while trying to append row. Error: {str(e)}"
                ) from e
