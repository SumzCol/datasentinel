from email.message import EmailMessage
from io import BytesIO, StringIO
from pathlib import Path
from typing import Literal
from zipfile import ZipFile

from jinja2 import Template
from pandas import DataFrame, ExcelWriter

from dataguard.notification.renderer.core import AbstractRenderer, RendererError
from dataguard.validation.failed_rows_dataset.core import AbstractFailedRowsDataset
from dataguard.validation.result import DataValidationResult


class TemplateEmailMessageRenderer(AbstractRenderer[EmailMessage]):
    _MAX_FAILED_ROWS_LIMIT = 1000

    def __init__(
        self,
        template_path: str,
        include_failed_rows: bool = False,
        failed_rows_type: Literal["csv", "excel"] = "csv",
        failed_rows_limit: int = 100,
    ):
        if not 0 < failed_rows_limit <= self._MAX_FAILED_ROWS_LIMIT:
            raise RendererError("Failed rows limit must be greater than 0 and less than 10000")

        if failed_rows_type not in {"csv", "excel"}:
            raise RendererError("Failed rows type must be 'csv' or 'excel'")

        if not Path(template_path).is_file():
            raise RendererError(f"Template '{template_path}' file does not exist")

        self._include_failed_records = include_failed_rows
        self._failed_rows_limit = failed_rows_limit
        self._failed_rows_type = failed_rows_type

        with open(template_path) as f:
            file_content = f.read()
        self._template = Template(file_content)

    def render(self, result: DataValidationResult) -> EmailMessage:
        message = EmailMessage()
        try:
            message.set_content(self._render_email_content(result=result), subtype="html")

            if self._include_failed_records:
                message = self._add_failed_records_attachment(message=message, result=result)

            return message
        except Exception as e:
            raise RendererError(f"Error while rendering email message: {e!s}") from e

    def _render_email_content(self, result: DataValidationResult) -> str:
        data_validation_dict = result.to_dict()

        return self._template.render(
            **data_validation_dict,
        )

    def _failed_rows_to_pandas(self, failed_rows_dataset: AbstractFailedRowsDataset) -> DataFrame:
        return DataFrame(failed_rows_dataset.to_dict(limit=self._failed_rows_limit))

    def _add_failed_records_attachment(
        self, message: EmailMessage, result: DataValidationResult
    ) -> EmailMessage:
        zip_buffer = BytesIO()

        with ZipFile(zip_buffer, "w") as zip_file:
            for failed_check in result.failed_checks:
                for failed_rule in failed_check.failed_rules:
                    if failed_rule.failed_rows_dataset is None:
                        continue

                    base_filename = (
                        f"{result.run_id}_{failed_check.name}_{failed_rule.rule}_failed_rows"
                    )

                    df_failed_rows = self._failed_rows_to_pandas(failed_rule.failed_rows_dataset)

                    if self._failed_rows_type == "excel":
                        buffer = self._failed_rows_to_excel(df_failed_rows=df_failed_rows)
                        # attachment_args = {
                        #     "maintype": "application",
                        #     "subtype": "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        #     "filename": f"{base_filename}.xlsx"
                        # }
                        zip_file.writestr(f"{base_filename}.xlsx", buffer.getvalue())
                    else:
                        buffer = self._failed_rows_to_csv(df_failed_rows=df_failed_rows)
                        # attachment_args = {
                        #     "maintype": "text",
                        #     "subtype": "csv",
                        #     "filename": f"{base_filename}.csv"
                        # }
                        zip_file.writestr(f"{base_filename}.csv", buffer.getvalue())

        message.add_attachment(
            zip_buffer.getvalue(),
            maintype="application",
            subtype="zip",
            filename="failed_rows.zip",
        )

        return message

    @staticmethod
    def _failed_rows_to_excel(df_failed_rows: DataFrame) -> BytesIO:
        buffer = BytesIO()
        with ExcelWriter(buffer) as writer:
            df_failed_rows.to_excel(writer)
        buffer.seek(0)

        return buffer

    @staticmethod
    def _failed_rows_to_csv(df_failed_rows: DataFrame) -> StringIO:
        buffer = StringIO()
        df_failed_rows.to_csv(buffer, index=False)
        buffer.seek(0)

        return buffer
