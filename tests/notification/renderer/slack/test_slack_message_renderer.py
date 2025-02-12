import json
from datetime import datetime
from typing import Any, List
from unittest.mock import Mock

import pytest
from ulid import ULID

from dataguard.notification.renderer.core import RendererError
from dataguard.notification.renderer.slack.slack_message_render import (
    SlackMessageRenderer,
)
from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.result import DataValidationResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


@pytest.fixture
def data_validation_result_mock():
    def _create(
        status: Status,
        time: datetime = None,
        failed_checks_mocks: List[Mock] = None,
    ) -> Mock:
        result = Mock(spec=DataValidationResult)
        result.run_id = ULID()
        result.status = status
        result.name = "test_data_validation"
        result.data_asset = "test_data_asset"
        result.data_asset_schema = "test_data_asset_schema"
        result.start_time = time if time else datetime.now()
        result.end_time = time if time else datetime.now()
        result.failed_checks = failed_checks_mocks

        result.failed_checks_count = len(failed_checks_mocks) if failed_checks_mocks else 0

        return result
    return _create


@pytest.fixture
def check_result_mock():
    def _create(
        status: Status,
        check_level: CheckLevel,
        class_name: str,
        failed_rules_mocks: List[Mock] = None,
    ) -> Mock:
        result = Mock(spec=CheckResult)
        result.name = "test_check"
        result.level = check_level
        result.status = status
        result.class_name = class_name
        result.failed_rules = failed_rules_mocks

        result.failed_rules_count = len(failed_rules_mocks) if failed_rules_mocks else 0
        return result
    return _create


@pytest.fixture
def rule_metric_mock():
    def _create(
        rule: str = "test_rule",
        status: Status = Status.FAIL,
        column: List[str] = None,
        violations: int = 0,
        rows: int = 0,
        value: Any = None
    ) -> Mock:
        result = Mock(spec=RuleMetric)
        result.status = status
        result.rule = rule
        result.column = column if column else ["test_column"]
        result.value = value
        result.violations = violations
        result.rows = rows

        return result
    return _create


class TestSlackMessageRendererUnit:
    def test_when_data_validation_pass(self, data_validation_result_mock):
        result = data_validation_result_mock(Status.PASS, datetime.now())
        slack_message_render = SlackMessageRenderer()

        message = slack_message_render.render(result)

        assert message.text == (
            f"test_data_validation data validation passed!, run id: {result.run_id}, "
            f"data asset: {result.data_asset}, "
            f"data asset schema: {result.data_asset_schema}, "
            f"start time: {result.start_time.isoformat()}, "
            f"end time: {result.end_time.isoformat()}."
        )
        assert len(message.blocks) == 2
        # Test blocks structure
        assert message.blocks[0]["type"] == "header"
        assert message.blocks[1]["type"] == "section"
        assert message.blocks[0]["text"]["text"] == "A data validation has passed!"

    @pytest.mark.parametrize(
        "checks_display_limit",
        [0, -1, 7],
        ids=["zero", "negative", "greater than 5"],
    )
    def test_with_bad_checks_display_limit_value(self, checks_display_limit: int):
        with pytest.raises(RendererError):
            SlackMessageRenderer(checks_display_limit=checks_display_limit)

    @pytest.mark.parametrize(
        "rules_display_limit",
        [0, -1, 7],
        ids=["zero", "negative", "greater than 5"],
    )
    def test_with_bad_rules_display_limit_value(self, rules_display_limit: int):
        with pytest.raises(RendererError):
            SlackMessageRenderer(rules_display_limit=rules_display_limit)

    def test_when_data_validation_failed(
        self,
        data_validation_result_mock,
        check_result_mock,
        rule_metric_mock,
    ):
        result = data_validation_result_mock(
            status=Status.FAIL,
            failed_checks_mocks=[
                check_result_mock(
                    status=Status.FAIL,
                    check_level=CheckLevel.CRITICAL,
                    class_name="test_check",
                    failed_rules_mocks=[
                        rule_metric_mock(
                            rule="test_rule",
                            status=Status.FAIL,
                            column=["test_column"],
                            violations=1,
                            rows=2,
                        ),
                        rule_metric_mock(
                            rule="test_rule2",
                            status=Status.FAIL,
                            column=["test_column", "test_column_2"],
                            violations=1,
                            rows=2,
                        ),
                        rule_metric_mock(
                            rule="is_custom",
                            status=Status.FAIL,
                            column=["test_column"],
                            value="package.module.function",
                            violations=1,
                            rows=2,
                        ),
                    ],
                )
            ],
        )

        slack_message_render = SlackMessageRenderer()

        message = slack_message_render.render(result)

        assert message.text == (
            f"test_data_validation data validation failed!, run id: {result.run_id}, "
            f"data asset: {result.data_asset}, "
            f"data asset schema: {result.data_asset_schema}, "
            f"start time: {result.start_time.isoformat()}, "
            f"end time: {result.end_time.isoformat()}. Failed checks: "
            f"test_check ([rule=test_rule, column=[test_column], violations=1, rows=2], "
            f"[rule=test_rule2, column=[test_column, test_column_2], violations=1, rows=2], "
            f"[rule=is_custom, value=package.module.function, violations=1, rows=2])"
        )
        # Test blocks structure
        assert len(message.blocks) == 4
        assert message.blocks[0]["type"] == "header"
        assert message.blocks[0]["text"]["text"] == "A data validation has failed! :alerta:"
        assert message.blocks[1]["type"] == "section"
        assert message.blocks[2]["text"]["text"] == "*Failed Checks*:"
        assert message.blocks[3]["type"] == "rich_text"
        assert len(message.blocks[3]["elements"]) == 5
        assert len(message.blocks[3]["elements"][4]["elements"]) == 3
        assert message.blocks[3]["elements"][4]["elements"][2]["elements"][0]["text"] == (
            "rule=is_custom, value=package.module.function, violations=1, rows=2"
        )
        assert message.blocks[3]["elements"][4]["elements"][0]["elements"][0]["text"] == (
            "rule=test_rule, column=[test_column], violations=1, rows=2"
        )

    def test_when_checks_display_limit_is_lower_than_failed_checks_count(
        self,
        data_validation_result_mock,
        check_result_mock,
        rule_metric_mock
    ):
        result = data_validation_result_mock(
            status=Status.FAIL,
            failed_checks_mocks=[
                check_result_mock(
                    status=Status.FAIL,
                    check_level=CheckLevel.CRITICAL,
                    class_name="test_check",
                    failed_rules_mocks=[
                        rule_metric_mock()
                    ],
                ),
                check_result_mock(
                    status=Status.FAIL,
                    check_level=CheckLevel.CRITICAL,
                    class_name="test_check2",
                    failed_rules_mocks=[
                        rule_metric_mock()
                    ],
                ),
            ],
        )

        slack_message_render = SlackMessageRenderer(
            checks_display_limit=1
        )

        message = slack_message_render.render(result)

        assert len(message.blocks) == 4
        assert message.blocks[2]["type"] == "section"
        assert message.blocks[2]["text"]["text"] == "*Failed Checks (Showing only 1 of 2)*:"

    def test_when_rules_display_limit_is_lower_than_failed_rules_count(
        self,
        data_validation_result_mock,
        check_result_mock,
        rule_metric_mock
    ):
        result = data_validation_result_mock(
            status=Status.FAIL,
            failed_checks_mocks=[
                check_result_mock(
                    status=Status.FAIL,
                    check_level=CheckLevel.CRITICAL,
                    class_name="test_check",
                    failed_rules_mocks=[
                        rule_metric_mock(),
                        rule_metric_mock()
                    ],
                ),
            ],
        )

        slack_message_render = SlackMessageRenderer(
            rules_display_limit=1
        )

        message = slack_message_render.render(result)

        with open("test_slack_message_renderer.json", "w") as f:
            f.write(json.dumps(message.blocks, indent=4))

        assert len(message.blocks) == 4
        assert message.blocks[3]["elements"][3]["elements"][0]["text"] == (
            "Failed rules (Showing only 1 of 2): "
        )



