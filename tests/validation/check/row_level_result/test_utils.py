import pytest

from datasentinel.validation.check.row_level_result.utils import (
    are_id_columns_in_rule_columns,
    evaluate_pass_rate,
)


@pytest.mark.unit
class TestUtilsUnit:
    def test_normal_case(self):
        """Test normal case where bad records are less than total rows."""
        total_rows = 100
        bad_records = 20
        expected_pass_rate = 0.8  # 1 - (20/100)
        assert evaluate_pass_rate(total_rows, bad_records) == expected_pass_rate

    def test_all_records_good(self):
        """Test case where all records are good (no bad records)."""
        total_rows = 50
        bad_records = 0
        expected_pass_rate = 1.0
        assert evaluate_pass_rate(total_rows, bad_records) == expected_pass_rate

    def test_all_records_bad(self):
        """Test case where all records are bad."""
        total_rows = 75
        bad_records = 75
        expected_pass_rate = 0.0
        assert evaluate_pass_rate(total_rows, bad_records) == expected_pass_rate

    def test_zero_total_rows(self):
        """Test case with zero total rows."""
        total_rows = 0
        bad_records = 0
        expected_pass_rate = 1.0  # Special case handling for zero division
        assert evaluate_pass_rate(total_rows, bad_records) == expected_pass_rate

    def test_exact_match(self):
        """Test case where id column matches a rule column exactly."""
        id_columns = ["customer_id", "order_id"]
        rule_columns = ["customer_id", "product_id"]
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is True

    def test_case_insensitive_match(self):
        """Test case with case insensitive matching."""
        id_columns = ["CUSTOMER_ID", "ORDER_ID"]
        rule_columns = ["customer_id", "product_id"]
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is True

    def test_no_match(self):
        """Test case where no id columns match rule columns."""
        id_columns = ["user_id", "account_id"]
        rule_columns = ["product_id", "inventory_id"]
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is False

    def test_empty_id_columns(self):
        """Test case with empty id columns list."""
        id_columns = []
        rule_columns = ["customer_id", "product_id"]
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is False

    def test_empty_rule_columns(self):
        """Test case with empty rule columns list."""
        id_columns = ["customer_id", "order_id"]
        rule_columns = []
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is False

    def test_string_rule_column(self):
        """Test case where rule_columns is a string instead of a list."""
        id_columns = ["customer_id", "order_id"]
        rule_columns = "customer_id"
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is True

    def test_string_rule_column_no_match(self):
        """Test case where rule_columns is a string with no match."""
        id_columns = ["customer_id", "order_id"]
        rule_columns = "product_id"
        assert are_id_columns_in_rule_columns(id_columns, rule_columns) is False
