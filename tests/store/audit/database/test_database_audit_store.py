from datetime import date, datetime
import json
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

from datasentinel.store.audit.core import AuditStoreError
from datasentinel.store.audit.database.database_audit_store import DatabaseAuditStore
from datasentinel.store.audit.row import BaseAuditRow, FieldInfo


@pytest.mark.unit
@pytest.mark.audit_store
class TestDatabaseAuditStore:
    @pytest.fixture
    def valid_credentials(self):
        """Valid credentials with connection string."""
        return {"connection_string": "sqlite:///:memory:"}

    @pytest.fixture
    def mock_audit_row(self):
        """Mock audit row with various field types."""
        row = Mock(spec=BaseAuditRow)
        row.row_fields = {
            "id": FieldInfo(annotation=int, type=int, args=None, required=True, complex=False),
            "name": FieldInfo(annotation=str, type=str, args=None, required=True, complex=False),
            "created_at": FieldInfo(
                annotation=datetime, type=datetime, args=None, required=True, complex=False
            ),
            "is_active": FieldInfo(
                annotation=bool, type=bool, args=None, required=True, complex=False
            ),
        }
        row.to_dict.return_value = {
            "id": 1,
            "name": "test",
            "created_at": datetime(2023, 1, 1, 12, 0, 0),
            "is_active": True,
        }
        return row

    def test_initialization_success(self, valid_credentials):
        """Test successful initialization with valid credentials."""
        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
        )

        assert store.name == "test_store"
        assert store.disabled is False

    def test_initialization_missing_connection_string(self):
        """Test initialization fails when connection string is missing."""
        invalid_credentials = {"username": "user", "password": "pass"}

        with pytest.raises(AuditStoreError, match="Connection string not found in credentials"):
            DatabaseAuditStore(
                name="test_store",
                disabled=False,
                table="audit_table",
                schema="test_schema",
                credentials=invalid_credentials,
            )

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    def test_append_successful_row_insertion(
        self,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
        mock_audit_row,
    ):
        """Test successful row append to database."""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        mock_table = Mock()
        mock_table_class.return_value = mock_table
        mock_insert_statement = Mock()
        mock_table.insert.return_value = mock_insert_statement
        mock_insert_statement.values.return_value = mock_insert_statement

        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
        )

        # Execute append - should complete successfully
        store.append(mock_audit_row)

        # Verify session operations
        mock_session.execute.assert_called_once_with(mock_insert_statement)
        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_not_called()

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    def test_append_database_error_raises_audit_store_error(
        self,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
        mock_audit_row,
    ):
        """Test that database errors during append raise AuditStoreError."""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        # Simulate database error during execution
        mock_session.execute.side_effect = SQLAlchemyError("Database connection failed")

        mock_table = Mock()
        mock_table_class.return_value = mock_table
        mock_insert_statement = Mock()
        mock_table.insert.return_value = mock_insert_statement
        mock_insert_statement.values.return_value = mock_insert_statement

        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
        )

        # Execute append - should raise AuditStoreError
        with pytest.raises(
            AuditStoreError, match="There was an error while trying to append row"
        ):
            store.append(mock_audit_row)

        # Verify rollback was called
        mock_session.rollback.assert_called_once()

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    def test_append_creates_table_when_not_exists_and_create_mode(
        self,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
        mock_audit_row,
    ):
        """
        Test that append creates table when it doesn't exist and if_table_not_exists='create'.
        """
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        # First call raises NoSuchTableError (table doesn't exist),
        # second call succeeds (table created)
        mock_table = Mock()
        mock_table_class.side_effect = [NoSuchTableError("Table not found"), mock_table]
        mock_insert_statement = Mock()
        mock_table.insert.return_value = mock_insert_statement
        mock_insert_statement.values.return_value = mock_insert_statement

        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
            if_table_not_exists="create",
        )

        # Execute append - should create table and complete successfully
        store.append(mock_audit_row)

        # Verify table creation was attempted (Table called twice)
        assert mock_table_class.call_count == 2
        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    def test_append_raises_error_when_table_not_exists_and_error_mode(
        self,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
        mock_audit_row,
    ):
        """
        Test that append raises error when table doesn't exist and if_table_not_exists='error'.
        """
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker

        # Table doesn't exist
        mock_table_class.side_effect = NoSuchTableError("Table not found")

        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
            if_table_not_exists="error",
        )

        # Execute append - should raise AuditStoreError
        with pytest.raises(AuditStoreError, match="Table 'audit_table' does not exist"):
            store.append(mock_audit_row)

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    def test_append_handles_complex_data_types(
        self, mock_table_class, mock_sessionmaker, mock_create_engine, valid_credentials
    ):
        """Test that append handles various data types correctly through field formatting."""
        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        mock_table = Mock()
        mock_table_class.return_value = mock_table
        mock_insert_statement = Mock()
        mock_table.insert.return_value = mock_insert_statement
        mock_insert_statement.values.return_value = mock_insert_statement

        # Create audit row with complex data types
        complex_row = Mock(spec=BaseAuditRow)
        complex_row.row_fields = {
            "datetime_field": FieldInfo(
                annotation=datetime, type=datetime, args=None, required=True, complex=False
            ),
            "date_field": FieldInfo(
                annotation=date, type=date, args=None, required=True, complex=False
            ),
            "dict_field": FieldInfo(
                annotation=dict, type=dict, args=None, required=True, complex=True
            ),
            "none_field": FieldInfo(
                annotation=str, type=str, args=None, required=False, complex=False
            ),
        }
        complex_row.to_dict.return_value = {
            "datetime_field": datetime(2023, 1, 1, 12, 0, 0),
            "date_field": date(2023, 1, 1),
            "dict_field": {"key": "value"},
            "none_field": None,
        }

        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="audit_table",
            schema="test_schema",
            credentials=valid_credentials,
        )

        # Execute append - should handle complex types
        store.append(complex_row)

        # Verify the insert was called with formatted values
        mock_insert_statement.values.assert_called_once()
        call_args = mock_insert_statement.values.call_args[0][0]

        # Verify datetime was formatted as ISO string
        assert call_args["datetime_field"] == "2023-01-01T12:00:00"
        # Verify date was formatted as ISO string
        assert call_args["date_field"] == "2023-01-01"
        # Verify dict was JSON serialized
        assert call_args["dict_field"] == json.dumps({"key": "value"})
        # Verify None remained None
        assert call_args["none_field"] is None

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    @patch("datasentinel.store.audit.database.database_audit_store.MetaData")
    def test_append_raises_audit_store_error_on_sqlalchemy_error_in_get_or_create_table(
        self,
        mock_metadata_class,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
    ):
        """
        Test that append raises AuditStoreError when SQLAlchemyError occurs in
        _get_or_create_table.
        """
        from sqlalchemy.exc import SQLAlchemyError

        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        # Mock Table to raise SQLAlchemyError (not NoSuchTableError)
        mock_table_class.side_effect = SQLAlchemyError("Database connection failed")

        # Create store and audit row
        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="test_table",
            schema="test_schema",
            credentials={"connection_string": "sqlite:///:memory:"},
            if_table_not_exists="create",
        )

        audit_row = Mock(spec=BaseAuditRow)
        audit_row.row_fields = {
            "field1": FieldInfo(annotation=str, type=str, args=None, required=True, complex=False)
        }
        audit_row.to_dict.return_value = {"field1": "value1"}

        # Test that SQLAlchemyError in _get_or_create_table raises AuditStoreError
        with pytest.raises(AuditStoreError) as exc_info:
            store.append(audit_row)

        # Verify the error message
        assert "There was an error while trying to get or create table 'test_table'" in str(
            exc_info.value
        )
        assert "Database connection failed" in str(exc_info.value)

    @patch("datasentinel.store.audit.database.database_audit_store.create_engine")
    @patch("datasentinel.store.audit.database.database_audit_store.sessionmaker")
    @patch("datasentinel.store.audit.database.database_audit_store.Table")
    @patch("datasentinel.store.audit.database.database_audit_store.MetaData")
    def test_append_raises_audit_store_error_on_sqlalchemy_error_in_create_table(
        self,
        mock_metadata_class,
        mock_table_class,
        mock_sessionmaker,
        mock_create_engine,
        valid_credentials,
    ):
        """
        Test that append raises AuditStoreError when SQLAlchemyError occurs in _create_table.
        """
        from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

        # Setup mocks
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        mock_session = Mock()
        mock_session_maker = Mock(return_value=mock_session)
        mock_sessionmaker.return_value = mock_session_maker
        mock_session.__enter__ = Mock(return_value=mock_session)
        mock_session.__exit__ = Mock(return_value=None)

        # Mock Table to raise NoSuchTableError first (to trigger table creation)
        # Then mock the second Table call (in _create_table) to raise SQLAlchemyError
        mock_table_class.side_effect = [
            NoSuchTableError("Table does not exist"),
            SQLAlchemyError("Failed to create table"),
        ]

        mock_metadata = Mock()
        mock_metadata_class.return_value = mock_metadata
        mock_metadata.create_all.side_effect = SQLAlchemyError("Failed to create table")

        # Create store and audit row
        store = DatabaseAuditStore(
            name="test_store",
            disabled=False,
            table="test_table",
            schema="test_schema",
            credentials={"connection_string": "sqlite:///:memory:"},
            if_table_not_exists="create",
        )

        audit_row = Mock(spec=BaseAuditRow)
        audit_row.row_fields = {
            "field1": FieldInfo(annotation=str, type=str, args=None, required=True, complex=False)
        }
        audit_row.to_dict.return_value = {"field1": "value1"}

        # Test that SQLAlchemyError in _create_table raises AuditStoreError
        with pytest.raises(AuditStoreError) as exc_info:
            store.append(audit_row)

        # Verify the error message
        assert "There was an error while trying to create table 'test_table'" in str(
            exc_info.value
        )
        assert "Failed to create table" in str(exc_info.value)
