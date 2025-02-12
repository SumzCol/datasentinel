from typing import List, Dict, Any

from pydantic.dataclasses import dataclass

from dataguard.notification.renderer.core import AbstractRenderer, RendererError
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@dataclass
class SlackMessage:
    text: str
    blocks: List[Dict[str, Any]]


class SlackMessageRenderer(AbstractRenderer[SlackMessage]):
    def __init__(self, checks_display_limit: int = 5, rules_display_limit: int = 5):
        if not 0 < checks_display_limit <= 5:
            raise RendererError("Checks display limit must be greater than 0 and less than 5.")
        if not 0 < rules_display_limit <= 5:
            raise RendererError("Rules display limit must be greater than 0 and less than 5.")
        self._checks_display_limit = checks_display_limit
        self._rules_display_limit = rules_display_limit

    def _render_text_rules_metric(self, rules_metric: List[RuleMetric]) -> str:
        return ", ".join(
            [
                f"{rule_metric.rule}: {', '.join(rule_metric.column)}"
                if not rule_metric.rule == "is_custom"
                else f"{rule_metric.rule}: {rule_metric.value}"
                for rule_metric in rules_metric[:self._rules_display_limit]
            ]
        )

    def _render_text_message(self, result: DataValidationResult) -> str:
        status = result.status
        status_str = "passed" if status == Status.PASS else "failed"
        message = (
            f"{result.name} data validation {status_str}!, run id: {result.run_id}, "
            f"data asset: {result.data_asset}, "
            f"data asset schema: {result.data_asset_schema}, "
            f"start time: {result.start_time.isoformat()}, "
            f"end time: {result.end_time.isoformat()}."
        )

        if status == Status.PASS:
            return message

        failed_checks_str = ", ".join(
            [
                (
                    f"{failed_check.name} "
                    f"({self._render_text_rules_metric(failed_check.failed_rules)})"
                )
                for failed_check in result.failed_checks[:self._checks_display_limit]
            ]
        )

        return f"{message} Failed checks: {failed_checks_str}"

    def _render_block_rules_metric(self, rules_metric: List[RuleMetric]) -> List[Dict[str, Any]]:
        return [
            {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "text",
                        "text": f"{rule_metric.rule}: "
                    },
                    {
                        "type": "text",
                        "text": (
                            ",".join(rule_metric.column)
                            if not rule_metric.rule == "is_custom"
                            else rule_metric.value
                        )
                    }
                ]
            }
            for rule_metric in rules_metric[:self._rules_display_limit]
        ]

    def _render_blocks_message(self, result: DataValidationResult) -> List[Dict[str, Any]]:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": (
                        "A data validation has passed!"
                        if result.status == Status.PASS
                        else "A data validation has failed! :alerta:"
                    ),
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join([
                        f"*Validation Name:* {result.name}",
                        f"*Run ID*: {result.run_id}",
                        f"*Data Asset*: {result.data_asset}",
                        f"*Data Asset Schema*: {result.data_asset_schema}",
                        f"*Start Time*: {result.start_time.isoformat()}",
                        f"*End Time*: {result.end_time.isoformat()}"
                    ])
                }
            },
        ]

        if result.status == Status.PASS:
            return blocks

        blocks.extend(
            [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "Check name: ",
                                    "style": {
                                        "bold": "true"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": failed_check.name,
                                }
                            ]
                        },
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "Check Class: ",
                                    "style": {
                                        "bold": "true"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": failed_check.class_name
                                }
                            ]
                        },
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "Check level: ",
                                    "style": {
                                        "bold": "true"
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": failed_check.level.value
                                }
                            ]
                        },
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "Failed rules: ",
                                    "style": {
                                        "bold": "true"
                                    }
                                }
                            ]
                        },
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                *self._render_block_rules_metric(failed_check.failed_rules)
                            ]
                        }
                    ]
                }
                for failed_check in result.failed_checks[:self._checks_display_limit]
            ]
        )

        return blocks

    def render(self, result: DataValidationResult) -> SlackMessage:
        return SlackMessage(
            text=self._render_text_message(result),
            blocks=self._render_blocks_message(result)
        )
