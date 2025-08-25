def is_equal_to_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.0,)], ["col"], "col", [], 1.0, 0),
                ([(2,)], ["col"], "col", ["col"], 2, 0),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 2.0, 0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.000001,)], ["col"], "col", [], 1.0, 1),
                ([(2,)], ["col"], "col", ["col"], 1, 1),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 1.0, 1),
            ],
            "ids": [
                "fail_without_id_columns",
                "fail_with_id_column_equal_to_evaluated_column",
                "fail_with_id_column_not_equal_to_evaluated_column",
            ],
        }


def is_greater_or_equal_to_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.0,)], ["col"], "col", [], 1.0, 0),
                ([(2,)], ["col"], "col", ["col"], 2, 0),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 0, 0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(0.9999999999,)], ["col"], "col", [], 1.0, 1),
                ([(1,)], ["col"], "col", ["col"], 2, 1),
                ([(1, 1.0000000000001)], ["id", "col"], "col", ["id"], 1.00000000001, 1),
            ],
            "ids": [
                "fail_without_id_columns",
                "fail_with_id_column_equal_to_evaluated_column",
                "fail_with_id_column_not_equal_to_evaluated_column",
            ],
        }


def is_greater_than_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.00000000001,)], ["col"], "col", [], 1.0, 0),
                ([(3,)], ["col"], "col", ["col"], 2, 0),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 1.99999999999, 0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(0.99999999,)], ["col"], "col", [], 1.0, 1),
                ([(1,)], ["col"], "col", ["col"], 2, 1),
                ([(1, 1.99999999)], ["id", "col"], "col", ["id"], 2.0, 1),
            ],
            "ids": [
                "fail_without_id_columns",
                "fail_with_id_column_equal_to_evaluated_column",
                "fail_with_id_column_not_equal_to_evaluated_column",
            ],
        }


def is_less_or_equal_to_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(0.99999999,)], ["col"], "col", [], 1.0, 0),
                ([(2,)], ["col"], "col", ["col"], 2, 0),
                ([(1, 1.0000001)], ["id", "col"], "col", ["id"], 1.000001, 0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.0,)], ["col"], "col", [], 0.99999999, 1),
                ([(3,)], ["col"], "col", ["col"], 2, 1),
                ([(1, 1.000001)], ["id", "col"], "col", ["id"], 1.0000001, 1),
            ],
            "ids": [
                "fail_without_id_columns",
                "fail_with_id_column_equal_to_evaluated_column",
                "fail_with_id_column_not_equal_to_evaluated_column",
            ],
        }


def is_less_than_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(0.99999999,)], ["col"], "col", [], 1.0, 0),
                ([(1,)], ["col"], "col", ["col"], 2, 0),
                ([(1, 1.0000001)], ["id", "col"], "col", ["id"], 1.000001, 0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, rule_value, expected_violations"
            ),
            "argvalues": [
                ([(1.000001,)], ["col"], "col", [], 0.99999999, 1),
                ([(3,)], ["col"], "col", ["col"], 2, 1),
                ([(1, 1.000001)], ["id", "col"], "col", ["id"], 1.0000001, 1),
            ],
            "ids": [
                "fail_without_id_columns",
                "fail_with_id_column_equal_to_evaluated_column",
                "fail_with_id_column_not_equal_to_evaluated_column",
            ],
        }
