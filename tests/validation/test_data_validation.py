from unittest.mock import Mock

from pydantic import ValidationError
import pytest

from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.data_asset.core import AbstractDataAsset
from dataguard.validation.data_validation import DataValidation


@pytest.mark.unit
class TestDataValidationUnit:
    def test_error_on_empty_check_list(self):
        with pytest.raises(ValidationError, match="Data validation must have at least one check"):
            DataValidation(
                name="test_data_validation",
                check_list=[],
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
