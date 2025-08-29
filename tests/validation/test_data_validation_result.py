from datetime import datetime, timedelta
from unittest.mock import Mock, PropertyMock

from pydantic import ValidationError
import pytest
from ulid import ULID

from datasentinel.validation.check.level import CheckLevel
from datasentinel.validation.check.result import CheckResult
from datasentinel.validation.result import DataValidationResult
from datasentinel.validation.status import Status


@pytest.mark.unit
class TestDataValidationResultUnit:
    @pytest.mark.parametrize(
        "expected_status",
        [
            Status.PASS,
            Status.FAIL,
        ],
    )
    def test_status_property(self, expected_status: Status):
        check_result = Mock(spec=CheckResult)
        type(check_result).status = PropertyMock(return_value=expected_status)

        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=[check_result],
        )

        assert result.status == expected_status

    def test_error_on_start_time_before_end_time(self):
        with pytest.raises(ValidationError, match="Start time must be before end time"):
            DataValidationResult(
                run_id=ULID(),
                name="test_name",
                data_asset="test_data_asset",
                start_time=datetime.now(),
                end_time=datetime.now() - timedelta(seconds=10),
                check_results=[],
            )

    def test_checks_count_property(self):
        check_result = Mock(spec=CheckResult)
        type(check_result).status = PropertyMock(return_value=Status.FAIL)

        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=[check_result],
        )

        assert result.failed_checks == [check_result]
        assert result.checks_count == 1

    def test_failed_checks_property(self):
        check_result = Mock(spec=CheckResult)
        type(check_result).status = PropertyMock(return_value=Status.FAIL)

        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=[check_result],
        )

        assert result.failed_checks == [check_result]

    def test_failed_checks_count_property(self):
        check_result = Mock(spec=CheckResult)
        type(check_result).status = PropertyMock(return_value=Status.FAIL)
        check_results = [check_result, check_result]
        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=check_results,
        )
        expected_failed_checks_count = len(check_results)

        assert result.failed_checks_count == expected_failed_checks_count

    @pytest.mark.parametrize(
        "level",
        [
            CheckLevel.ERROR,
            CheckLevel.WARNING,
            CheckLevel.CRITICAL,
        ],
    )
    def test_failed_checks_by_level(self, level: CheckLevel):
        check_result = Mock(spec=CheckResult)
        check_result.level = level
        type(check_result).status = PropertyMock(return_value=Status.FAIL)
        check_results = [check_result]
        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=check_results,
        )

        assert result.failed_checks_by_level(level) == check_results

    def test_to_dict(self):
        result = DataValidationResult(
            run_id=ULID(),
            name="test_name",
            data_asset="test_data_asset",
            start_time=datetime.now(),
            end_time=datetime.now(),
            check_results=[],
        )

        expected_dict = {
            "run_id": result.run_id,
            "name": result.name,
            "data_asset": result.data_asset,
            "data_asset_schema": result.data_asset_schema,
            "start_time": result.start_time,
            "end_time": result.end_time,
            "check_results": [],
            "status": result.status,
        }

        assert result.to_dict() == expected_dict
