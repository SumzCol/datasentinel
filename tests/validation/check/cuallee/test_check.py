from unittest.mock import MagicMock, patch

from cuallee import CheckLevel as CualleeCheckLevel
import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.check.level import CheckLevel


@pytest.mark.unit
class TestCualleeCheckUnit:
    @pytest.mark.parametrize(
        "level, expected_cuallee_level",
        [
            (CheckLevel.WARNING, CualleeCheckLevel.WARNING),
            (CheckLevel.ERROR, CualleeCheckLevel.ERROR),
            (CheckLevel.CRITICAL, CualleeCheckLevel.ERROR),
        ],
        ids=["warning", "error", "critical"],
    )
    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_cuallee_check_level(self, mock_cuallee_check, level, expected_cuallee_level):
        CualleeCheck(name="cuallee_check", level=level)

        mock_cuallee_check.assert_called_once_with(
            level=expected_cuallee_level, name="cuallee_check"
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_complete_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_complete method of the CualleeCheck class is called with the correct
        arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance
        (
            CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_complete(
                column="col", pct=0.5
            )
        )

        mock_check_instance.is_complete.assert_called_once_with(column="col", pct=0.5)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_empty_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_empty method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_empty(
            column="col", pct=0.8
        )

        mock_check_instance.is_empty.assert_called_once_with(column="col", pct=0.8)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_are_complete_rule_correct_definition(self, mock_cuallee_check):
        """Test that the are_complete method is called with the correct list argument."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        columns = ["col1", "col2", "col3"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).are_complete(
            column=columns, pct=0.9
        )

        mock_check_instance.are_complete.assert_called_once_with(column=columns, pct=0.9)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_unique_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_unique method is called with multiple arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_unique(
            column="col", pct=0.7, approximate=True, ignore_nulls=True
        )

        mock_check_instance.is_unique.assert_called_once_with(
            column="col", pct=0.7, approximate=True, ignore_nulls=True
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_primary_key_correct_definition(self, mock_cuallee_check):
        """Test that the is_primary_key method is called with multiple arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_primary_key(
            column="col", pct=0.7
        )

        mock_check_instance.is_primary_key.assert_called_once_with(column="col", pct=0.7)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_are_unique_rule_correct_definition(self, mock_cuallee_check):
        """Test that the are_unique method is called with list columns."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        columns = ["col1", "col2"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).are_unique(
            column=columns, pct=0.95
        )

        mock_check_instance.are_unique.assert_called_once_with(column=columns, pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_composite_key_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_composite_key method is called with list columns."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        columns = ["col1", "col2"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_composite_key(
            column=columns, pct=0.95
        )

        mock_check_instance.is_composite_key.assert_called_once_with(column=columns, pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_greater_than_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_greater_than method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_greater_than(
            column="col", value=10, pct=0.95
        )

        mock_check_instance.is_greater_than.assert_called_once_with(
            column="col", value=10, pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_positive_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_positive method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_positive(
            column="col", pct=0.95
        )

        mock_check_instance.is_positive.assert_called_once_with(column="col", pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_greater_or_equal_to_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_greater_or_equal_to method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_greater_or_equal_to(
            column="col", value=10, pct=0.95
        )

        mock_check_instance.is_greater_or_equal_than.assert_called_once_with(
            column="col", value=10, pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_in_millions_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_in_millions method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_in_millions(
            column="col", pct=0.95
        )

        mock_check_instance.is_in_millions.assert_called_once_with(column="col", pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_in_billions_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_in_billions method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_in_billions(
            column="col", pct=0.95
        )

        mock_check_instance.is_in_billions.assert_called_once_with(column="col", pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_less_than_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_less_than method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_less_than(
            column="col", value=10, pct=0.95
        )

        mock_check_instance.is_less_than.assert_called_once_with(column="col", value=10, pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_negative_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_negative method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_negative(
            column="col", pct=0.95
        )

        mock_check_instance.is_negative.assert_called_once_with(column="col", pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_less_or_equal_to_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_less_or_equal_to method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_less_or_equal_to(
            column="col", value=10, pct=0.95
        )

        mock_check_instance.is_less_or_equal_than.assert_called_once_with(
            column="col", value=10, pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_equal_to_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_equal_to method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_equal_to(
            column="col", value=10, pct=0.95
        )

        mock_check_instance.is_equal_than.assert_called_once_with(
            column="col", value=10, pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_pattern_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_pattern method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_pattern(
            column="col", value="pattern", pct=0.95
        )

        mock_check_instance.has_pattern.assert_called_once_with(
            column="col", value="pattern", pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_legit_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_legit method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_legit(
            column="col", pct=0.95
        )

        mock_check_instance.is_legit.assert_called_once_with(column="col", pct=0.95)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_min_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_min method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_min(
            column="col", value=10
        )

        mock_check_instance.has_min.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_max_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_max method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_max(
            column="col", value=10
        )

        mock_check_instance.has_max.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_std_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_std method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_std(
            column="col", value=10
        )

        mock_check_instance.has_std.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_mean_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_mean method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_mean(
            column="col", value=10
        )

        mock_check_instance.has_mean.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_sum_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_sum method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_sum(
            column="col", value=10
        )

        mock_check_instance.has_sum.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_between_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_between method correctly converts list to tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        value_range = [10, 20]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_between(
            column="col", value=value_range, pct=0.99
        )

        mock_check_instance.is_between.assert_called_once_with(
            column="col", value=tuple(value_range), pct=0.99
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_not_contained_in_rule_correct_definition(self, mock_cuallee_check):
        """Test that the not_contained_in method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        valid_values = ["A", "B", "C"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).not_contained_in(
            column="col", value=valid_values, pct=0.85
        )

        mock_check_instance.not_contained_in.assert_called_once_with(
            column="col", value=valid_values, pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_not_in_rule_correct_definition(self, mock_cuallee_check):
        """Test that the not_in method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        valid_values = ["A", "B", "C"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).not_in(
            column="col", value=valid_values, pct=0.85
        )

        mock_check_instance.not_in.assert_called_once_with(
            column="col", value=tuple(valid_values), pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_contained_in_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_contained_in method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        valid_values = ["A", "B", "C"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_contained_in(
            column="col", value=valid_values, pct=0.85
        )

        mock_check_instance.is_contained_in.assert_called_once_with(
            column="col", value=valid_values, pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_in_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_in method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        valid_values = ["A", "B", "C"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_in(
            column="col", value=valid_values, pct=0.85
        )

        mock_check_instance.is_in.assert_called_once_with(
            column="col", value=tuple(valid_values), pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_t_minus_n_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_t_minus_n method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_t_minus_n(
            column="col", value=1, pct=0.85, options={"col": "asc"}
        )

        mock_check_instance.is_t_minus_n.assert_called_once_with(
            column="col", value=1, pct=0.85, options={"col": "asc"}
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_t_minus_1_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_t_minus_1 method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_t_minus_1(
            column="col", pct=0.85
        )

        mock_check_instance.is_t_minus_1.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_t_minus_2_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_t_minus_2 method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_t_minus_2(
            column="col", pct=0.85
        )

        mock_check_instance.is_t_minus_2.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_t_minus_3_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_t_minus_3 method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_t_minus_3(
            column="col", pct=0.85
        )

        mock_check_instance.is_t_minus_3.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_yesterday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_yesterday method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_yesterday(
            column="col", pct=0.85
        )

        mock_check_instance.is_yesterday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_today_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_today method handles both list and tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_today(
            column="col", pct=0.85
        )

        mock_check_instance.is_today.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_percentile_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_percentile method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_percentile(
            column="col", value=50.5, percentile=0.75, precision=5000
        )

        mock_check_instance.has_percentile.assert_called_once_with(
            column="col", value=50.5, percentile=0.75, precision=5000
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_inside_interquartile_range_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_inside_interquartile_range method is called with the correct
        arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        value_range = [10.0, 90.0]
        CualleeCheck(
            name="cuallee_check", level=CheckLevel.WARNING
        ).is_inside_interquartile_range(column="col", value=value_range, pct=0.95)

        mock_check_instance.is_inside_interquartile_range.assert_called_once_with(
            column="col", value=value_range, pct=0.95
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_max_by_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_max_by method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_max_by(
            column_source="source_col", column_target="target_col", value=100.5
        )

        mock_check_instance.has_max_by.assert_called_once_with(
            column_source="source_col", column_target="target_col", value=100.5
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_min_by_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_min_by method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_min_by(
            column_source="source_col", column_target="target_col", value="min_value"
        )

        mock_check_instance.has_min_by.assert_called_once_with(
            column_source="source_col", column_target="target_col", value="min_value"
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_correlation_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_correlation method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_correlation(
            column_left="left_col", column_right="right_col", value=0.75
        )

        mock_check_instance.has_correlation.assert_called_once_with(
            column_left="left_col", column_right="right_col", value=0.75
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_satisfies_rule_correct_definition(self, mock_cuallee_check):
        """Test that the satisfies method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        options = {"option1": "value1"}
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).satisfies(
            column="col", predicate="x > 0", pct=0.9, options=options
        )

        mock_check_instance.satisfies.assert_called_once_with(
            column="col", predicate="x > 0", pct=0.9, options=options
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_cardinality_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_cardinality method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_cardinality(
            column="col", value=10
        )

        mock_check_instance.has_cardinality.assert_called_once_with(column="col", value=10)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_infogain_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_infogain method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_infogain(
            column="col", pct=0.8
        )

        mock_check_instance.has_infogain.assert_called_once_with(column="col", pct=0.8)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_entropy_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_entropy method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_entropy(
            column="col", value=0.75, tolerance=0.02
        )

        mock_check_instance.has_entropy.assert_called_once_with(
            column="col", value=0.75, tolerance=0.02
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_weekday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_weekday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_weekday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_weekday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_weekend_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_weekend method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_weekend(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_weekend.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_monday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_monday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_monday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_monday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_tuesday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_tuesday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_tuesday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_tuesday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_wednesday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_wednesday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_wednesday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_wednesday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_thursday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_thursday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_thursday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_thursday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_friday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_friday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_friday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_friday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_saturday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_saturday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_saturday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_saturday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_sunday_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_sunday method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_sunday(
            column="col", pct=0.85
        )

        mock_check_instance.is_on_sunday.assert_called_once_with(column="col", pct=0.85)

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_on_schedule_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_on_schedule method correctly converts list to tuple."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        schedule_values = ["9:00", "12:00", "17:00"]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_on_schedule(
            column="col", value=schedule_values, pct=0.85
        )

        # Verify list was converted to tuple
        mock_check_instance.is_on_schedule.assert_called_once_with(
            column="col", value=tuple(schedule_values), pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_daily_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_daily method is called with the correct arguments."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        days_values = [1, 2, 3]  # Monday, Tuesday, Wednesday
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_daily(
            column="col", value=days_values, pct=0.85
        )

        mock_check_instance.is_daily.assert_called_once_with(
            column="col", value=days_values, pct=0.85
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_has_workflow_rule_correct_definition(self, mock_cuallee_check):
        """Test that the has_workflow method with complex parameters."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        edges = [("start", "middle"), ("middle", "end")]
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).has_workflow(
            column_group="group_col",
            column_event="event_col",
            column_order="order_col",
            edges=edges,
            pct=0.75,
        )

        mock_check_instance.has_workflow.assert_called_once_with(
            column_group="group_col",
            column_event="event_col",
            column_order="order_col",
            edges=edges,
            pct=0.75,
        )

    @patch("datasentinel.validation.check.cuallee.check.Check")
    def test_is_custom_rule_correct_definition(self, mock_cuallee_check):
        """Test that the is_custom method with a function parameter."""
        mock_check_instance = MagicMock()
        mock_cuallee_check.return_value = mock_check_instance

        def custom_function(df):
            return df

        options = {"option1": "value1"}
        CualleeCheck(name="cuallee_check", level=CheckLevel.WARNING).is_custom(
            column="col", fn=custom_function, pct=0.6, options=options
        )

        mock_check_instance.is_custom.assert_called_once_with(
            column="col", fn=custom_function, pct=0.6, options=options
        )
