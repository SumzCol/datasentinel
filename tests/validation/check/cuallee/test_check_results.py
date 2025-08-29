from pandas import DataFrame as PandasDataFrame
import pytest

from datasentinel.validation.check import CualleeCheck
from datasentinel.validation.check.level import CheckLevel
from datasentinel.validation.status import Status


@pytest.mark.unit
class TestCualleeCheckResultsUnit:
    @pytest.mark.pandas
    def test_correct_check_results_with_pandas_df(self):
        """Test that the CheckResult is created with the correct values when the evaluated data
        is a pandas DataFrame."""
        df = PandasDataFrame({"col": [1, 2, 3]})
        check_result = (
            CualleeCheck(
                name="test_name",
                level=CheckLevel.ERROR,
            )
            .is_greater_than(column="col", value=2)
            .validate(df)
        )

        assert check_result.name == "test_name"
        assert check_result.level == CheckLevel.ERROR
        assert check_result.class_name == "CualleeCheck"

        rule_metric = check_result.rule_metrics[0]
        assert rule_metric.id == 1
        assert rule_metric.rule == "is_greater_than"
        assert rule_metric.column == ["col"]
        assert rule_metric.value == 2
        assert rule_metric.function is None
        assert rule_metric.rows == 3
        assert rule_metric.violations == 2
        assert rule_metric.pass_rate == 1.0 / 3.0
        assert rule_metric.pass_threshold == 1.0
        assert rule_metric.status == Status.FAIL
        assert rule_metric.options is None
        assert rule_metric.failed_rows_dataset is None

    @pytest.mark.pyspark
    def test_correct_check_results_with_pyspark_df(self, spark):
        """Test that the CheckResult is created with the correct values when the evaluated data
        is a pyspark DataFrame."""
        df = spark.createDataFrame([{"col": 1}, {"col": 2}, {"col": 3}])
        check_result = (
            CualleeCheck(
                name="test_name",
                level=CheckLevel.ERROR,
            )
            .is_greater_than(column="col", value=2)
            .validate(df)
        )

        assert check_result.name == "test_name"
        assert check_result.level == CheckLevel.ERROR
        assert check_result.class_name == "CualleeCheck"

        rule_metric = check_result.rule_metrics[0]
        assert rule_metric.id == 1
        assert rule_metric.rule == "is_greater_than"
        assert rule_metric.column == ["col"]
        assert rule_metric.value == 2
        assert rule_metric.function is None
        assert rule_metric.rows == 3
        assert rule_metric.violations == 2
        assert rule_metric.pass_threshold == 1.0
        assert rule_metric.status == Status.FAIL
        assert rule_metric.options is None
        assert rule_metric.failed_rows_dataset is None

    def test_is_custom_check_results(self):
        """Test that the CheckResult is created with the correct values when the evaluated data
        is a pandas DataFrame and the rule is a custom rule."""
        df = PandasDataFrame({"col": [1, 2, 3]})

        def custom_rule(df):
            return df["col"] > 2

        check_result = (
            CualleeCheck(
                name="test_name",
                level=CheckLevel.ERROR,
            )
            .is_custom(column="col", fn=custom_rule, options={"op": "greater_than"})
            .validate(df)
        )

        assert check_result.name == "test_name"
        assert check_result.level == CheckLevel.ERROR
        assert check_result.class_name == "CualleeCheck"

        rule_metric = check_result.rule_metrics[0]
        assert rule_metric.id == 1
        assert rule_metric.rule == "is_custom"
        assert rule_metric.column == ["col"]
        assert rule_metric.value is None
        assert rule_metric.function == custom_rule
        assert rule_metric.rows == 3
        assert rule_metric.violations == 2
        assert rule_metric.pass_rate == 1.0 / 3.0
        assert rule_metric.pass_threshold == 1.0
        assert rule_metric.status == Status.FAIL
        assert rule_metric.options == {"op": "greater_than"}
        assert rule_metric.failed_rows_dataset is None
