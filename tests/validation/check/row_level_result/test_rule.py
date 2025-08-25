from typing import Any

from pydantic import ValidationError
import pytest

from datasentinel.validation.check.row_level_result.rule import Rule, RuleDataType


@pytest.mark.unit
class TestRuleClassUnit:
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
                method="method",
                data_type=RuleDataType.AGNOSTIC,
                value=[1, "a", "2022-01-01"],
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
                method="rule_method",
                data_type=data_type,
                value=value,
            )

    def test_empty_columns_and_id_columns(self):
        rule = Rule(
            method="rule_method",
            data_type=RuleDataType.AGNOSTIC,
        )

        assert rule.column == []
        assert rule.id_columns == []

    @pytest.mark.parametrize(
        "column, id_columns, expected_queried_columns",
        [
            (["col1", "col2"], ["id1", "id2"], ["id1", "id2", "col1", "col2"]),
            (["col1", "col2"], [], ["col1", "col2"]),
            (["col1", "col2", "id"], ["id"], ["id", "col1", "col2"]),
        ],
    )
    def test_queried_columns_property(
        self, column: list[str], id_columns: list[str], expected_queried_columns: list[str]
    ):
        rule = Rule(
            method="rule_method",
            data_type=RuleDataType.AGNOSTIC,
            column=column,
            id_columns=id_columns,
        )

        assert rule.queried_columns == expected_queried_columns
