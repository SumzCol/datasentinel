from email.message import EmailMessage
from unittest.mock import Mock, patch

import pytest

from dataguard.notification.renderer.core import RendererError
from dataguard.notification.renderer.email.template_email_message_renderer import (
    TemplateEmailMessageRenderer,
)
from dataguard.validation.result import DataValidationResult
from dataguard.validation.status import Status


@pytest.mark.unit
@pytest.mark.renderer
class TestTemplateEmailMessageRenderer:
    @pytest.fixture
    def mock_validation_result_pass(self):
        """Mock validation result with PASS status."""
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.name = "test_validation"
        result.status = Status.PASS
        result.failed_checks = []
        result.run_id = "test_run_123"
        return result

    @pytest.fixture
    def mock_validation_result_fail(self):
        """Mock validation result with FAIL status."""
        result = Mock(spec=DataValidationResult)
        result.data_asset = "test_table"
        result.name = "test_validation"
        result.status = Status.FAIL
        result.failed_checks = []
        result.run_id = "test_run_123"
        return result

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_initialization_with_default_template(self, mock_read_text):
        """Test successful initialization with default template."""
        mock_read_text.return_value = "<html>{{result.data_asset}}</html>"

        TemplateEmailMessageRenderer()

        mock_read_text.assert_called_once_with(
            "dataguard.notification.renderer.email.templates", "default.html"
        )

    @patch("dataguard.notification.renderer.email.template_email_message_renderer.Path")
    def test_initialization_with_custom_template(self, mock_path_class):
        """Test successful initialization with custom template path."""
        mock_path = Mock()
        mock_path.read_text.return_value = "<html>{{result.data_asset}}</html>"
        mock_path_class.return_value = mock_path

        TemplateEmailMessageRenderer(template_path="/custom/template.html")

        mock_path_class.assert_called_once_with("/custom/template.html")
        mock_path.read_text.assert_called_once()

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_initialization_template_not_found_default(self, mock_read_text):
        """Test initialization fails when default template is not found."""
        mock_read_text.side_effect = FileNotFoundError("Template not found")

        with pytest.raises(RendererError):
            TemplateEmailMessageRenderer()

    @patch("dataguard.notification.renderer.email.template_email_message_renderer.Path")
    def test_initialization_template_not_found_custom(self, mock_path_class):
        """Test initialization fails when custom template is not found."""
        mock_path = Mock()
        mock_path.read_text.side_effect = FileNotFoundError("Template not found")
        mock_path_class.return_value = mock_path

        with pytest.raises(RendererError):
            TemplateEmailMessageRenderer(template_path="/custom/template.html")

    def test_initialization_invalid_file_type(self):
        """Test initialization fails with invalid failed_rows_file_type."""
        with pytest.raises(RendererError):
            TemplateEmailMessageRenderer(failed_rows_file_type="invalid")

    def test_initialization_invalid_failed_rows_limit_zero(self):
        """Test initialization fails with failed_rows_limit of zero."""
        with pytest.raises(RendererError):
            TemplateEmailMessageRenderer(failed_rows_limit=0)

    def test_initialization_invalid_failed_rows_limit_too_high(self):
        """Test initialization fails with failed_rows_limit too high."""
        with pytest.raises(RendererError):
            TemplateEmailMessageRenderer(failed_rows_limit=100001)

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_pass_status_email(self, mock_read_text, mock_validation_result_pass):
        """Test rendering email for PASS status validation."""
        mock_read_text.return_value = "<html>Status: {{result.status.value}}</html>"

        renderer = TemplateEmailMessageRenderer()
        result = renderer.render(mock_validation_result_pass)

        assert isinstance(result, EmailMessage)
        assert "test_table" in result["Subject"]
        content = result.get_content()
        assert "Status: PASS" in content

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_fail_status_email(self, mock_read_text, mock_validation_result_fail):
        """Test rendering email for FAIL status validation."""
        mock_read_text.return_value = "<html>Status: {{result.status.value}}</html>"

        renderer = TemplateEmailMessageRenderer()
        result = renderer.render(mock_validation_result_fail)

        assert isinstance(result, EmailMessage)
        assert "test_table" in result["Subject"]
        content = result.get_content()
        assert "Status: FAIL" in content

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_with_failed_rows_attachment_excel(
        self, mock_read_text, mock_validation_result_fail
    ):
        """Test rendering email with failed rows attachment in Excel format."""
        mock_read_text.return_value = "<html>{{result.data_asset}}</html>"

        # Setup failed check with failed rows
        mock_failed_check = Mock()
        mock_failed_check.name = "test_check"
        mock_failed_rule = Mock()
        mock_failed_rule.id = "rule_1"
        mock_failed_rule.rule = "not_null"
        mock_failed_rule.failed_rows_dataset = Mock()
        # Make to_dict return the same data regardless of limit parameter
        mock_failed_rule.failed_rows_dataset.to_dict = Mock(
            return_value={"col1": [1, 2], "col2": ["a", "b"]}
        )
        mock_failed_check.failed_rules = [mock_failed_rule]
        mock_validation_result_fail.failed_checks = [mock_failed_check]

        renderer = TemplateEmailMessageRenderer(
            include_failed_rows=True, failed_rows_file_type="excel"
        )

        # Mock ExcelWriter as a context manager
        mock_excel_writer = Mock()
        mock_excel_writer.__enter__ = Mock(return_value=mock_excel_writer)
        mock_excel_writer.__exit__ = Mock(return_value=None)

        with (
            patch(
                "dataguard.notification.renderer.email.template_email_message_renderer.ExcelWriter",
                return_value=mock_excel_writer,
            ),
            patch(
                "dataguard.notification.renderer.email.template_email_message_renderer.DataFrame"
            ) as mock_df_class,
        ):
            # Mock DataFrame creation
            mock_df = Mock()
            mock_df_class.return_value = mock_df
            mock_df.to_excel = Mock()

            result = renderer.render(mock_validation_result_fail)

        assert isinstance(result, EmailMessage)
        # Check that attachment was added
        attachments = [part for part in result.iter_attachments()]
        assert len(attachments) == 1
        assert attachments[0].get_filename().endswith(".zip")

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_with_failed_rows_attachment_csv(
        self, mock_read_text, mock_validation_result_fail
    ):
        """Test rendering email with failed rows attachment in CSV format."""
        mock_read_text.return_value = "<html>{{result.data_asset}}</html>"

        # Setup failed check with failed rows
        mock_failed_check = Mock()
        mock_failed_check.name = "test_check"
        mock_failed_rule = Mock()
        mock_failed_rule.id = "rule_1"
        mock_failed_rule.rule = "not_null"
        mock_failed_rule.failed_rows_dataset = Mock()
        # Make to_dict return the same data regardless of limit parameter
        mock_failed_rule.failed_rows_dataset.to_dict = Mock(
            return_value={"col1": [1, 2], "col2": ["a", "b"]}
        )
        mock_failed_check.failed_rules = [mock_failed_rule]
        mock_validation_result_fail.failed_checks = [mock_failed_check]

        renderer = TemplateEmailMessageRenderer(
            include_failed_rows=True, failed_rows_file_type="csv"
        )

        with patch(
            "dataguard.notification.renderer.email.template_email_message_renderer.DataFrame"
        ) as mock_df_class:
            # Mock DataFrame creation
            mock_df = Mock()
            mock_df_class.return_value = mock_df
            mock_df.to_csv = Mock()

            result = renderer.render(mock_validation_result_fail)

        assert isinstance(result, EmailMessage)
        # Check that attachment was added
        attachments = [part for part in result.iter_attachments()]
        assert len(attachments) == 1
        assert attachments[0].get_filename().endswith(".zip")

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_no_attachment_when_no_failed_rows(
        self, mock_read_text, mock_validation_result_fail
    ):
        """Test that no attachment is added when there are no failed rows datasets."""
        mock_read_text.return_value = "<html>{{result.data_asset}}</html>"

        # Setup failed check without failed rows dataset
        mock_failed_check = Mock()
        mock_failed_check.name = "test_check"
        mock_failed_rule = Mock()
        mock_failed_rule.failed_rows_dataset = None
        mock_failed_check.failed_rules = [mock_failed_rule]
        mock_validation_result_fail.failed_checks = [mock_failed_check]

        renderer = TemplateEmailMessageRenderer(include_failed_rows=True)
        result = renderer.render(mock_validation_result_fail)

        assert isinstance(result, EmailMessage)
        # Check that no attachment was added
        attachments = list(result.iter_attachments())
        assert len(attachments) == 0

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_template_error_handling(self, mock_read_text, mock_validation_result_pass):
        """Test that template rendering errors are properly handled."""
        mock_read_text.return_value = "<html>{{result.invalid_property}}</html>"

        renderer = TemplateEmailMessageRenderer()

        # This should not raise an exception even with invalid template property
        # Jinja2 will just render empty string for undefined variables
        result = renderer.render(mock_validation_result_pass)
        assert isinstance(result, EmailMessage)

    @patch(
        "dataguard.notification.renderer.email.template_email_message_renderer.resources.read_text"
    )
    def test_render_general_error_handling(self, mock_read_text, mock_validation_result_pass):
        """Test that general rendering errors raise RendererError."""
        mock_read_text.return_value = "<html>{{result.data_asset}}</html>"

        renderer = TemplateEmailMessageRenderer()

        # Force an error by making the result None
        with pytest.raises(RendererError, match="Error while rendering email message"):
            renderer.render(None)
