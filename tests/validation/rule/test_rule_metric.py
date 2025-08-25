from pydantic import ValidationError
import pytest

from datasentinel.validation.rule.metric import RuleMetric
from datasentinel.validation.status import Status


@pytest.mark.unit
class TestRuleMetricUnit:
    def test_error_on_violations_greater_than_rows(self):
        with pytest.raises(ValidationError, match="Violations cannot be greater than rows"):
            RuleMetric(
                id=1,
                rule="test_rule",
                rows=10,
                violations=11,
                pass_rate=0.9,
                pass_threshold=0.8,
            )

    def test_function_to_string(self):
        def function():
            pass

        assert (
            RuleMetric.function_to_string(function)
            == f"{function.__module__}.{function.__name__}"
        )

    @pytest.mark.parametrize(
        "pass_rate, pass_threshold, expected_status",
        [
            (0.9, 0.8, Status.PASS),
            (0.9, 0.9, Status.PASS),
            (0.7, 0.8, Status.FAIL),
        ],
    )
    def test_status(self, pass_rate: float, pass_threshold: float, expected_status: Status):
        rule_metric = RuleMetric(
            id=1,
            rule="test_rule",
            rows=10,
            violations=0,
            pass_rate=pass_rate,
            pass_threshold=pass_threshold,
        )

        assert rule_metric.status == expected_status

    def test_to_dict(self):
        args = {
            "id": 1,
            "rule": "test_rule",
            "column": ["col1", "col2"],
            "id_columns": ["id1", "id2"],
            "value": 1,
            "function": None,
            "rows": 10,
            "violations": 0,
            "pass_rate": 1.0,
            "pass_threshold": 1.0,
            "options": {"option": "value"},
            "failed_rows_dataset": None,
        }
        rule_metric = RuleMetric(**args)

        assert rule_metric.to_dict() == {
            **args,
            "status": Status.PASS,
        }
