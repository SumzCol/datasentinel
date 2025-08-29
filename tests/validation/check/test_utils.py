from unittest.mock import MagicMock, patch

import pytest

from datasentinel.validation.check.utils import get_type, to_df_if_delta_table


@pytest.mark.unit
class TestUtilsUnit:
    def test_get_type_with_builtin_types(self):
        """Test get_type function with built-in Python types."""
        # Test with a list
        assert get_type([1, 2, 3]) == "list"

        # Test with a dict
        assert get_type({"a": 1}) == "dict"

        # Test with a string
        assert get_type("test_string") == "str"

        # Test with an int
        assert get_type(42) == "int"

    def test_get_type_with_custom_class(self):
        """Test get_type function with a custom class."""

        class TestClass:
            pass

        obj = TestClass()
        assert "TestClass" in get_type(obj)

    @patch("datasentinel.validation.check.utils.get_type")
    def test_to_df_if_delta_table_with_delta(self, mock_get_type):
        """Test to_df_if_delta_table with a Delta table."""
        # Mock get_type to return a string containing 'delta'
        mock_get_type.return_value = "delta.tables.DeltaTable"

        # Create a mock Delta table
        mock_delta = MagicMock()
        mock_delta.toDF.return_value = "converted_dataframe"

        result = to_df_if_delta_table(mock_delta)

        # Check that toDF was called
        mock_delta.toDF.assert_called_once()
        # Check the return value
        assert result == "converted_dataframe"

    @patch("datasentinel.validation.check.utils.get_type")
    def test_to_df_if_delta_table_with_non_delta(self, mock_get_type):
        """Test to_df_if_delta_table with a non-Delta object."""
        # Mock get_type to return a string without 'delta'
        mock_get_type.return_value = "pyspark.sql.dataframe.DataFrame"

        # Create a regular dataframe mock
        mock_df = MagicMock()

        result = to_df_if_delta_table(mock_df)

        # Check that the object was returned unchanged
        assert result == mock_df
        # Ensure toDF was never called
        mock_df.toDF.assert_not_called()
