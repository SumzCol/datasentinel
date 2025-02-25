from typing import Any

import pytest
from pydantic import ValidationError

from dataguard.validation.check.row_level_result.rule import Rule, RuleDataType


class TestRuleClass:
    @pytest.mark.parametrize("pass_threshold", [-2.0, 2.0])
    def test_error_on_invalid_pass_threshold(self, pass_threshold: float):
        with pytest.raises(ValidationError):
            Rule(
                method="is_complete",
                data_type=RuleDataType.AGNOSTIC,
                pass_threshold=pass_threshold,
            )

    def test_error_on_list_as_value_with_different_types_of_elements(self):
        with pytest.raises(ValidationError):
            Rule(
                method="is_between",
                data_type=RuleDataType.AGNOSTIC,
                value=[1, "2"],
            )

    @pytest.mark.parametrize(
        "data_type, value",
        [
            (RuleDataType.NUMERIC, "1"),
            (RuleDataType.TIMESTAMP, "2022-01-01"),
            (RuleDataType.DATE, "2022-01-01"),
            (RuleDataType.STRING, 1),
        ],
    )
    def test_error_on_invalid_scalar_value_data_type(self, data_type: RuleDataType, value: Any):
        with pytest.raises(ValidationError):
            Rule(
                method="test",
                data_type=data_type,
                value=value,
            )
