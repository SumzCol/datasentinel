from typing import List, Dict, Any

from pydantic.dataclasses import dataclass

from dataguard.notification.renderer.core import AbstractRenderer
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@dataclass
class SlackMessage:
    text: str
    blocks: List[Dict[str, Any]]


class SlackMessageRender(AbstractRenderer[SlackMessage]):

    @staticmethod
    def _render_text_message(result: DataValidationResult) -> str:
        status_str = "passed" if result.status == Status.PASS else "failed"
        return (
            f"{result.name} data validation {status_str}!, run id: {result.run_id}, "
            f"data asset: {result.data_asset}, "
            f"data asset schema: {result.data_asset_schema}, "
            f"start time: {result.start_time.isoformat()}, "
            f"end time: {result.end_time.isoformat()}"
        )

    @staticmethod
    def _render_rules_metric(rules_metric: List[RuleMetric]) -> List[Dict[str, Any]]:
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
            for rule_metric in rules_metric
        ]

    @staticmethod
    def _render_blocks_message(result: DataValidationResult) -> List[Dict[str, Any]]:
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
                                    "text": check_result.name,
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
                                    "text": check_result.class_name
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
                                    "text": check_result.level.value
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
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {
                                            "type": "text",
                                            "text": f"{rule.rule}: "
                                        },
                                        {
                                            "type": "text",
                                            "text": ",".join(rule.column)
                                        }
                                    ]
                                }
                                for rule in check_result.failed_rules
                            ]
                        }
                    ]
                }
                for check_result in result.failed_checks
            ]
        )

        return blocks

    def render(self, result: DataValidationResult) -> SlackMessage:
        return SlackMessage(
            text=self._render_text_message(result),
            blocks=self._render_blocks_message(result)
        )
