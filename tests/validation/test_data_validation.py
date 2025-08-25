from datetime import datetime
from unittest.mock import Mock, patch

from pydantic import ValidationError
import pytest
from ulid import ULID

from datasentinel.validation.check.core import AbstractCheck
from datasentinel.validation.check.result import CheckResult
from datasentinel.validation.data_asset.core import AbstractDataAsset
from datasentinel.validation.data_validation import DataValidation
from datasentinel.validation.result import DataValidationResult


@pytest.mark.unit
class TestDataValidationUnit:
    def test_error_on_empty_check_list(self):
        with pytest.raises(ValidationError, match="Data validation must have at least one check"):
            DataValidation(
                name="test_data_validation",
                check_list=[],
                data_asset=Mock(spec=AbstractDataAsset),
            )

    def test_error_on_duplicate_check_names(self):
        """Test that DataValidation raises ValueError when check_list contains checks with
        duplicate names."""
        # Create two checks with the same name
        check1 = Mock(spec=AbstractCheck)
        check1.name = "duplicate_check"

        check2 = Mock(spec=AbstractCheck)
        check2.name = "duplicate_check"

        # Should raise ValueError for duplicate check names
        with pytest.raises(ValueError, match="Data validation checks must have unique names"):
            DataValidation(
                name="test_data_validation",
                check_list=[check1, check2],
                data_asset=Mock(spec=AbstractDataAsset),
            )

    def test_checks_count(self):
        check_list = [Mock(spec=AbstractCheck), Mock(spec=AbstractCheck)]
        data_validation = DataValidation(
            name="test_data_validation",
            check_list=check_list,
            data_asset=Mock(spec=AbstractDataAsset),
        )
        expected_count = len(check_list)

        assert data_validation.checks_count == expected_count

    def test_check_exists(self):
        check_list = [Mock(spec=AbstractCheck), Mock(spec=AbstractCheck)]
        data_validation = DataValidation(
            name="test_data_validation",
            check_list=check_list,
            data_asset=Mock(spec=AbstractDataAsset),
        )
        check_name = check_list[0].name

        assert data_validation.check_exists(check_name)
        assert not data_validation.check_exists("non_existing_check")

    @patch("datasentinel.validation.data_validation.datetime")
    @patch("datasentinel.validation.data_validation.ULID")
    def test_run_successful_execution(self, mock_ulid, mock_datetime):
        """Test that run method successfully orchestrates data loading, check execution,
        and result creation."""
        # Setup mock datetime
        mock_start_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_end_time = datetime(2023, 1, 1, 12, 5, 0)
        mock_datetime.now.side_effect = [mock_start_time, mock_end_time]

        # Setup mock ULID
        mock_run_id = ULID()
        mock_ulid.return_value = mock_run_id

        # Setup mock data asset
        mock_data_asset = Mock(spec=AbstractDataAsset)
        mock_data_asset.name = "test_table"
        mock_data_asset.schema = "test_schema"
        mock_data = Mock()  # Mock loaded data
        mock_data_asset.load.return_value = mock_data

        # Setup mock check and its result
        mock_check = Mock(spec=AbstractCheck)
        mock_check_result = Mock(spec=CheckResult)
        mock_check.validate.return_value = mock_check_result

        # Create DataValidation instance
        data_validation = DataValidation(
            name="test_validation", check_list=[mock_check], data_asset=mock_data_asset
        )

        # Execute run method
        result = data_validation.run()

        # Verify data asset load was called
        mock_data_asset.load.assert_called_once()

        # Verify check validate was called with loaded data
        mock_check.validate.assert_called_once_with(mock_data)

        # Verify result structure
        assert isinstance(result, DataValidationResult)
        assert result.run_id == mock_run_id
        assert result.name == "test_validation"
        assert result.data_asset == "test_table"
        assert result.data_asset_schema == "test_schema"
        assert result.start_time == mock_start_time
        assert result.end_time == mock_end_time
        assert result.check_results == [mock_check_result]

    @patch("datasentinel.validation.data_validation.datetime")
    @patch("datasentinel.validation.data_validation.ULID")
    def test_run_multiple_checks_execution(self, mock_ulid, mock_datetime):
        """Test that run method executes all checks and collects their results properly."""
        # Setup mock datetime
        mock_start_time = datetime(2023, 1, 1, 12, 0, 0)
        mock_end_time = datetime(2023, 1, 1, 12, 5, 0)
        mock_datetime.now.side_effect = [mock_start_time, mock_end_time]

        # Setup mock ULID
        mock_run_id = ULID()
        mock_ulid.return_value = mock_run_id

        # Setup mock data asset
        mock_data_asset = Mock(spec=AbstractDataAsset)
        mock_data_asset.name = "test_table"
        mock_data_asset.schema = "test_schema"
        mock_data = Mock()  # Mock loaded data
        mock_data_asset.load.return_value = mock_data

        # Setup multiple mock checks and their results
        mock_check1 = Mock(spec=AbstractCheck)
        mock_check1.name = "completeness_check"
        mock_check_result1 = Mock(spec=CheckResult)
        mock_check1.validate.return_value = mock_check_result1

        mock_check2 = Mock(spec=AbstractCheck)
        mock_check2.name = "uniqueness_check"
        mock_check_result2 = Mock(spec=CheckResult)
        mock_check2.validate.return_value = mock_check_result2

        # Create DataValidation instance with multiple checks
        data_validation = DataValidation(
            name="comprehensive_validation",
            check_list=[mock_check1, mock_check2],
            data_asset=mock_data_asset,
        )

        # Execute run method
        result = data_validation.run()

        # Verify data asset load was called once
        mock_data_asset.load.assert_called_once()

        # Verify all checks were executed with the same loaded data
        mock_check1.validate.assert_called_once_with(mock_data)
        mock_check2.validate.assert_called_once_with(mock_data)

        # Verify result contains all check results in correct order
        assert isinstance(result, DataValidationResult)
        assert result.run_id == mock_run_id
        assert result.name == "comprehensive_validation"
        assert result.data_asset == "test_table"
        assert result.data_asset_schema == "test_schema"
        assert result.start_time == mock_start_time
        assert result.end_time == mock_end_time
        assert len(result.check_results) == 2  # noqa: PLR2004
        assert result.check_results == [
            mock_check_result1,
            mock_check_result2,
        ]
