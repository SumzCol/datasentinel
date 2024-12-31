from typing import Callable, List

from dataguard.validation.check.row_level_check.rule import Rule


def evaluate_pass_rate(total_rows: int, bad_records: int) -> float:
    if bad_records <= total_rows:
        try:
            return 1 - (bad_records / total_rows)
        except ZeroDivisionError:
            return 1.0
    else:
        try:
            return total_rows / bad_records
        except ZeroDivisionError:
            return 0.0


def value(rule: Rule) -> str:
    """Removes verbosity for Callable values"""
    if isinstance(rule.value, Callable):
        return f"{rule.value.__module__}.{rule.value.__name__}"
    else:
        return str(rule.value)


def are_id_columns_in_rule_columns(
    id_columns: str | List[str],
    rule_columns: str | List[str]
) -> bool:
    if isinstance(rule_columns, str):
        rule_columns = [rule_columns]

    if isinstance(id_columns, str):
        id_columns = [id_columns]

    rule_columns = [col.lower() for col in rule_columns]
    return any([col.lower() in rule_columns for col in id_columns])