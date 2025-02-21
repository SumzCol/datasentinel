import pytest
from pydantic import ValidationError

from dataguard.validation.check.row_level_result.rule import Rule, CheckDataType


class TestRuleClass:
    @pytest.mark.parametrize("pass_threshold", [-2.0, 2.0])
    def test_invalid_pass_threshold(self, pass_threshold: float):
        with pytest.raises(ValidationError):
            Rule(
                method="is_complete",
                data_type=CheckDataType.AGNOSTIC,
                pass_threshold=pass_threshold,
            )

    def test_list_as_value_with_different_types_of_elements(self):
        with pytest.raises(ValidationError):
            Rule(
                method="is_between",
                data_type=CheckDataType.AGNOSTIC,
                value=[1, "2"],
            )
