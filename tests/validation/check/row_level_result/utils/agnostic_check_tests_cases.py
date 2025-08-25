from datetime import date, datetime


def are_complete_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": "data, columns, evaluated_column, id_columns, expected_violations",
            "argvalues": [
                ([(1, 1, 1), (2, 2, 2)], ["id", "col1", "col2"], ["col1", "col2"], ["id"], 0),
                (
                    [(1, "a", "a"), (2, "b", "b")],
                    ["id", "col1", "col2"],
                    ["col1", "col2"],
                    ["id"],
                    0,
                ),
                (
                    [(1, date.today(), date.today()), (2, date.today(), date.today())],
                    ["id", "col1", "col2"],
                    ["col1", "col2"],
                    ["id"],
                    0,
                ),
            ],
            "ids": [
                "pass_with_numeric_columns",
                "pass_with_string_columns",
                "pass_with_date_columns",
            ],
        }
    else:
        return {
            "argnames": "data, columns, evaluated_column, id_columns, expected_violations",
            "argvalues": [
                (
                    [(1, 2, None), (2, None, 3)],
                    ["id", "col1", "col2"],
                    ["col1", "col2"],
                    ["id"],
                    2,
                ),
                (
                    [(1, "b", None), (2, None, "c")],
                    ["id", "col1", "col2"],
                    ["col1", "col2"],
                    ["id"],
                    2,
                ),
                (
                    [(1, date.today(), None), (2, None, date.today())],
                    ["id", "col1", "col2"],
                    ["col1", "col2"],
                    ["id"],
                    2,
                ),
            ],
            "ids": [
                "fail_with_numeric_columns",
                "fail_with_string_columns",
                "fail_with_date_columns",
            ],
        }


def are_unique_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": "data, columns, evaluated_column, ignore_nulls",
            "argvalues": [
                ([(1, 1), (2, 2)], ["col1", "col2"], ["col1", "col2"], False),
                ([("a", "a"), ("b", "b")], ["col1", "col2"], ["col1", "col2"], False),
                (
                    [(date(2020, 1, 1), date(2020, 1, 1)), (date(2020, 1, 2), date(2020, 1, 2))],
                    ["col1", "col2"],
                    ["col1", "col2"],
                    False,
                ),
                ([(None, None), (None, None)], ["col1", "col2"], ["col1", "col2"], True),
            ],
            "ids": [
                "pass_with_numeric_columns",
                "pass_with_string_columns",
                "pass_with_date_columns",
                "pass_while_ignoring_nulls",
            ],
        }
    else:
        return {
            "argnames": "data, columns, evaluated_column, ignore_nulls, expected_violations",
            "argvalues": [
                ([(1, 1.0000001), (1, 1.0000001)], ["col1", "col2"], ["col1", "col2"], False, 1),
                (
                    [("a", "a"), ("a", "a"), ("b", "b"), ("b", "b")],
                    ["col1", "col2"],
                    ["col1", "col2"],
                    False,
                    2,
                ),
                (
                    [(date.today(), date.today()), (date.today(), date.today())],
                    ["col1", "col2"],
                    ["col1", "col2"],
                    False,
                    1,
                ),
                ([(None, None), (None, None)], ["col1", "col2"], ["col1", "col2"], False, 1),
            ],
            "ids": [
                "fail_with_numeric_columns",
                "fail_with_string_columns",
                "fail_with_date_columns",
                "fail_while_not_ignoring_nulls",
            ],
        }


def is_between_tests_cases_parameterize(pass_outcome: bool) -> dict:
    if pass_outcome:
        return {
            "argnames": "data, columns, evaluated_column, min_value, max_value",
            "argvalues": [
                ([(1.00000001,)], ["col1"], "col1", 1.0, 2.0),
                ([("b",)], ["col1"], "col1", "a", "c"),
                ([(date(2020, 1, 1),)], ["col1"], "col1", date(2020, 1, 1), date(2020, 1, 2)),
                (
                    [(datetime(2020, 1, 2),)],
                    ["col1"],
                    "col1",
                    datetime(2020, 1, 1),
                    datetime(2020, 1, 2),
                ),
            ],
            "ids": [
                "pass_with_numeric_columns",
                "pass_with_string_columns",
                "pass_with_date_columns",
                "pass_with_datetime_columns",
            ],
        }
    else:
        return {
            "argnames": (
                "data, columns, evaluated_column, id_columns, min_value, max_value, "
                "expected_violations"
            ),
            "argvalues": [
                (
                    [
                        (
                            1,
                            0.999999999999,
                        )
                    ],
                    ["id", "col1"],
                    "col1",
                    ["id"],
                    1.0,
                    2.0,
                    1,
                ),
                (
                    [
                        (
                            1,
                            "d",
                        )
                    ],
                    ["id", "col1"],
                    "col1",
                    ["id"],
                    "a",
                    "c",
                    1,
                ),
                (
                    [
                        (
                            1,
                            date(2020, 1, 3),
                        )
                    ],
                    ["id", "col1"],
                    "col1",
                    ["id"],
                    date(2020, 1, 1),
                    date(2020, 1, 2),
                    1,
                ),
                (
                    [
                        (
                            1,
                            datetime(2020, 1, 2, 0, 0, 1),
                        )
                    ],
                    ["id", "col1"],
                    "col1",
                    ["id"],
                    datetime(2020, 1, 1),
                    datetime(2020, 1, 2, 0, 0, 0),
                    1,
                ),
            ],
            "ids": [
                "fail_with_numeric_columns",
                "fail_with_string_columns",
                "fail_with_date_columns",
                "fail_with_datetime_columns",
            ],
        }
