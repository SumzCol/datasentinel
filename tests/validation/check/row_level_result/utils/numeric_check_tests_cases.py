def is_equal_to_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": ("data, columns, evaluated_column, id_columns, rule_value"),
            "argvalues": [
                ([(1.0,)], ["col"], "col", [], 1.0),
                ([(2,)], ["col"], "col", ["col"], 2),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 2.0),
            ],
            "ids": [
                "pass_without_id_columns",
                "pass_with_id_column_equal_to_evaluated_column",
                "pass_with_id_column_not_equal_to_evaluated_column",
            ],
        }
    else:
        return {
            "argnames": "data, columns, evaluated_column, id_columns, rule_value",
            "argvalues": [
                ([(1.000001,)], ["col"], "col", [], 1.0),
                ([(2,)], ["col"], "col", ["col"], 1),
                ([(1, 2.0)], ["id", "col"], "col", ["id"], 1.0),
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
            "argnames": "data, evaluated_column, rule_value",
            "argvalues": [[(1.0,)], "col", 1.0],
            "ids": ["pass"],
        }
    else:
        return {
            "argnames": "data, evaluated_column, rule_value",
            "argvalues": [[(1.000001,)], "col", 1.0],
            "ids": ["fail"],
        }
