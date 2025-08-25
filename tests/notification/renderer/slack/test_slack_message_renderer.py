from datetime import datetime
from unittest.mock import Mock

import pytest

from datasentinel.notification.renderer.core import RendererError
from datasentinel.notification.renderer.slack.slack_message_renderer import (
    SlackMessage,
    SlackMessageRenderer,
)
from datasentinel.validation.check.level import CheckLevel
from datasentinel.validation.result import DataValidationResult
from datasentinel.validation.status import Status


@pytest.mark.unit
@pytest.mark.renderer
class TestSlackMessageRenderer:
    @pytest.fixture
    def mock_validation_result_pass(self):
        """Mock validation result with PASS status."""
        result = Mock(spec=DataValidationResult)
        result.name = "test_validation"
        result.run_id = "run_123"
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.status = Status.PASS
        result.start_time = datetime(2023, 1, 1, 12, 0, 0)
        result.end_time = datetime(2023, 1, 1, 12, 5, 0)
        result.failed_checks = []
        result.failed_checks_count = 0
        return result

    @pytest.fixture
    def mock_validation_result_fail(self):
        """Mock validation result with FAIL status."""
        result = Mock(spec=DataValidationResult)
        result.name = "test_validation"
        result.run_id = "run_123"
        result.data_asset = "test_table"
        result.data_asset_schema = "test_schema"
        result.status = Status.FAIL
        result.start_time = datetime(2023, 1, 1, 12, 0, 0)
        result.end_time = datetime(2023, 1, 1, 12, 5, 0)
        result.failed_checks_count = 2
        return result

    def test_initialization_success(self):
        """Test successful initialization with valid parameters."""
        renderer = SlackMessageRenderer(checks_display_limit=3, rules_display_limit=4)

        # Test that renderer is created without errors
        assert renderer is not None

    def test_initialization_invalid_checks_limit(self):
        """Test initialization fails with invalid checks display limit."""
        with pytest.raises(RendererError, match="Checks display limit must be greater than 0"):
            SlackMessageRenderer(checks_display_limit=0)

        with pytest.raises(RendererError, match="less than 5"):
            SlackMessageRenderer(checks_display_limit=6)

    def test_initialization_invalid_rules_limit(self):
        """Test initialization fails with invalid rules display limit."""
        with pytest.raises(RendererError, match="Rules display limit must be greater than 0"):
            SlackMessageRenderer(rules_display_limit=0)

        with pytest.raises(RendererError, match="less than 5"):
            SlackMessageRenderer(rules_display_limit=6)

    def test_render_pass_status_contains_essential_info(self, mock_validation_result_pass):
        """Test that PASS status message contains all essential information."""
        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_pass)

        assert isinstance(result, SlackMessage)

        # Test that essential information is present in text
        assert "test_validation" in result.text
        assert "run_123" in result.text
        assert "test_table" in result.text
        assert "test_schema" in result.text
        assert "passed" in result.text

        # Test that blocks contain essential structure
        # At least header and section
        assert len(result.blocks) >= 2  # noqa: PLR2004
        # Check that blocks are present without checking internal structure
        assert result.blocks is not None

    def test_render_fail_status_contains_essential_info(self, mock_validation_result_fail):
        """Test that FAIL status message contains all essential information."""
        # Setup a simple failed check
        mock_failed_check = Mock()
        mock_failed_check.name = "completeness_check"
        mock_failed_check.level = CheckLevel.ERROR
        mock_failed_check.failed_rules = []
        mock_failed_check.failed_rules_count = 0
        mock_validation_result_fail.failed_checks = [mock_failed_check]

        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_fail)

        assert isinstance(result, SlackMessage)

        # Test that essential information is present in text
        assert "test_validation" in result.text
        assert "run_123" in result.text
        assert "test_table" in result.text
        assert "failed" in result.text
        assert "completeness_check" in result.text

        # Test that blocks contain failure-specific structure
        # More blocks for failure case
        assert len(result.blocks) > 2  # noqa: PLR2004
        assert result.blocks is not None

    def test_render_respects_checks_display_limit(self, mock_validation_result_fail):
        """Test that checks display limit is respected."""
        # Create more failed checks than the limit
        failed_checks = []
        for i in range(5):
            mock_check = Mock()
            mock_check.name = f"check_{i}"
            mock_check.level = CheckLevel.ERROR
            mock_check.failed_rules = []
            mock_check.failed_rules_count = 0
            failed_checks.append(mock_check)

        mock_validation_result_fail.failed_checks = failed_checks
        mock_validation_result_fail.failed_checks_count = 5

        renderer = SlackMessageRenderer(checks_display_limit=2)
        result = renderer.render(mock_validation_result_fail)

        # Should only contain first 2 checks in text
        assert "check_0" in result.text
        assert "check_1" in result.text
        assert "check_2" not in result.text

    def test_render_respects_rules_display_limit(self, mock_validation_result_fail):
        """Test that rules display limit is respected."""
        # Create failed check with multiple rules
        mock_rule_metrics = []
        for i in range(5):
            mock_rule = Mock()
            mock_rule.id = i
            mock_rule.rule = f"rule_{i}"
            mock_rule.column = ["test_col"]
            mock_rule.rows = 100
            mock_rule.violations = 10
            mock_rule_metrics.append(mock_rule)

        mock_failed_check = Mock()
        mock_failed_check.name = "test_check"
        mock_failed_check.level = CheckLevel.ERROR
        mock_failed_check.failed_rules = mock_rule_metrics
        mock_failed_check.failed_rules_count = 5

        mock_validation_result_fail.failed_checks = [mock_failed_check]
        mock_validation_result_fail.failed_checks_count = 1

        renderer = SlackMessageRenderer(rules_display_limit=2)
        result = renderer.render(mock_validation_result_fail)

        # Should only contain first 2 rules in text
        assert "rule_0" in result.text
        assert "rule_1" in result.text
        assert "rule_2" not in result.text

    def test_render_handles_custom_rules(self, mock_validation_result_fail):
        """Test that custom rules are handled differently from column rules."""
        # Create custom rule
        custom_rule = Mock()
        custom_rule.id = 1
        custom_rule.rule = "is_custom"
        custom_rule.value = "custom.function"
        custom_rule.rows = 100
        custom_rule.violations = 5

        # Create regular rule
        regular_rule = Mock()
        regular_rule.id = 2
        regular_rule.rule = "not_null"
        regular_rule.column = ["email"]
        regular_rule.rows = 100
        regular_rule.violations = 3

        mock_failed_check = Mock()
        mock_failed_check.name = "test_check"
        mock_failed_check.level = CheckLevel.ERROR
        mock_failed_check.failed_rules = [custom_rule, regular_rule]
        mock_failed_check.failed_rules_count = 2

        mock_validation_result_fail.failed_checks = [mock_failed_check]

        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_fail)

        # Custom rule should show value, regular rule should show column
        assert "value: custom.function" in result.text
        assert "column: [email]" in result.text

    def test_render_handles_missing_schema(self, mock_validation_result_pass):
        """Test that missing data asset schema is handled gracefully."""
        mock_validation_result_pass.data_asset_schema = None

        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_pass)

        # Should still render successfully
        assert isinstance(result, SlackMessage)
        assert "test_validation" in result.text
        # Schema should not appear in text when None
        assert "data asset schema: None" not in result.text.lower()

    def test_render_message_structure_consistency(
        self, mock_validation_result_pass, mock_validation_result_fail
    ):
        """Test that both PASS and FAIL messages have consistent basic structure."""
        # Add empty failed_checks for fail result to avoid errors
        mock_validation_result_fail.failed_checks = []

        renderer = SlackMessageRenderer()

        pass_result = renderer.render(mock_validation_result_pass)
        fail_result = renderer.render(mock_validation_result_fail)

        # Both should have SlackMessage structure
        assert isinstance(pass_result, SlackMessage)
        assert isinstance(fail_result, SlackMessage)

        # Both should have text and blocks
        assert hasattr(pass_result, "text") and pass_result.text
        assert hasattr(pass_result, "blocks") and pass_result.blocks
        assert hasattr(fail_result, "text") and fail_result.text
        assert hasattr(fail_result, "blocks") and fail_result.blocks

        # Both should have at least basic blocks (header + section)
        assert len(pass_result.blocks) >= 2  # noqa: PLR2004
        assert len(fail_result.blocks) >= 2  # noqa: PLR2004

    def test_render_includes_timing_information(self, mock_validation_result_pass):
        """Test that timing information is included in rendered message."""
        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_pass)

        # Should contain ISO formatted timestamps
        assert "2023-01-01T12:00:00" in result.text  # start time
        assert "2023-01-01T12:05:00" in result.text  # end time

    def test_render_includes_run_identification(self, mock_validation_result_pass):
        """Test that run identification information is included."""
        renderer = SlackMessageRenderer()
        result = renderer.render(mock_validation_result_pass)

        # Should contain run identification
        assert "run_123" in result.text
        assert "test_validation" in result.text
        assert "test_table" in result.text
