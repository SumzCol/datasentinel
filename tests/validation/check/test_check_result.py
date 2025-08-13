from datetime import datetime

import pytest

from dataguard.validation.check.level import CheckLevel
from dataguard.validation.check.result import CheckResult
from dataguard.validation.rule.metric import RuleMetric
from dataguard.validation.status import Status


def create_rule_metric(status: Status) -> RuleMetric:
    return RuleMetric(
        id=1,
        rule="test_rule",
        rows=10,
        violations=0 if status == Status.PASS else 1,
        pass_rate=1.0 if status == Status.PASS else 0.1,
        pass_threshold=1.0,
    )


@pytest.mark.unit
@pytest.mark.checks
class TestCheckResultUnit:
    @pytest.mark.parametrize("expected_status", [Status.PASS, Status.FAIL])
    def test_status_property(self, expected_status: Status):
        rule_metric = create_rule_metric(expected_status)

        check_result = CheckResult(
            name="test_name",
            rule_metrics=[rule_metric],
            class_name="test_class_name",
            level=CheckLevel.ERROR,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        assert check_result.status == expected_status

    def test_failed_rules_property(self):
        rule_metric_pass = create_rule_metric(Status.FAIL)
        rule_metrics = [rule_metric_pass]

        check_result = CheckResult(
            name="test_name",
            rule_metrics=rule_metrics,
            class_name="test_class_name",
            level=CheckLevel.ERROR,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        assert check_result.failed_rules == rule_metrics

    def test_failed_rules_count_property(self):
        rule_metric_pass = create_rule_metric(Status.FAIL)
        rule_metrics = [rule_metric_pass]
        expected_failed_rules_count = len(rule_metrics)

        check_result = CheckResult(
            name="test_name",
            rule_metrics=rule_metrics,
            class_name="test_class_name",
            level=CheckLevel.ERROR,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        assert check_result.failed_rules_count == expected_failed_rules_count

    def test_to_dict(self):
        check_result = CheckResult(
            name="test_name",
            rule_metrics=[],
            class_name="test_class_name",
            level=CheckLevel.ERROR,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        assert check_result.to_dict() == {
            "name": check_result.name,
            "level": check_result.level.name,
            "check_class": check_result.class_name,
            "start_time": check_result.start_time,
            "end_time": check_result.end_time,
            "rule_metrics": [],
            "status": check_result.status,
        }
