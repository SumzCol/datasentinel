from datetime import date, datetime
from unittest.mock import Mock

from pandas import DataFrame
import pytest

from dataguard.validation.check.row_level_result.pandas_strategy import PandasValidationStrategy
from dataguard.validation.check.row_level_result.rule import Rule, RuleDataType


@pytest.fixture(scope="function")
def rule_mock():
    def _rule(column: list[str], data_type: RuleDataType):
        rule_mock = Mock(spec=Rule)
        rule_mock.method = "method"
        rule_mock.column = column
        rule_mock.data_type = data_type
        return rule_mock

    return _rule


@pytest.mark.unit
class TestValidateDataTypesUnit:
    @pytest.mark.parametrize(
        "data",
        [
            [("1",)],
            [(date(2020, 1, 1),)],
            [(datetime(2020, 1, 1),)],
        ],
    )
    def test_error_on_numeric_rule_with_invalid_column_data_type(
        self, rule_mock, data: list[tuple]
    ):
        evaluated_column = "col"
        rule = rule_mock(column=[evaluated_column], data_type=RuleDataType.NUMERIC)
        df = DataFrame(data=data, columns=[evaluated_column])
        strategy = PandasValidationStrategy()

        with pytest.raises(TypeError):
            strategy.validate_data_types(df, {"key": rule})

    @pytest.mark.parametrize(
        "data",
        [
            [(1,)],
            [(1.0,)],
            [(date(2020, 1, 1),)],
            [(datetime(2020, 1, 1),)],
        ],
    )
    def test_error_on_string_rule_with_invalid_column_data_type(
        self, rule_mock, data: list[tuple]
    ):
        evaluated_column = "col"
        rule = rule_mock(column=[evaluated_column], data_type=RuleDataType.STRING)
        df = DataFrame(data=data, columns=[evaluated_column])
        strategy = PandasValidationStrategy()

        with pytest.raises(TypeError):
            strategy.validate_data_types(df, {"key": rule})

    @pytest.mark.parametrize(
        "data",
        [
            [("2020-01-01",)],
            [(1,)],
            [(1.0,)],
        ],
    )
    def test_error_on_timestamp_or_date_rule_with_invalid_column_data_type(
        self, rule_mock, data: list[tuple]
    ):
        evaluated_column = "col"
        rule = rule_mock(column=[evaluated_column], data_type=RuleDataType.TIMESTAMP)
        df = DataFrame(data=data, columns=[evaluated_column])
        strategy = PandasValidationStrategy()

        with pytest.raises(TypeError):
            strategy.validate_data_types(df, {"key": rule})
