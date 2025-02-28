import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.check.core import BadArgumentError
from dataguard.validation.check.level import CheckLevel


@pytest.mark.unit
class TestRowLevelResultCheckUnit:
    def test_is_complete_id_columns_in_evaluated_columns(self):
        with pytest.raises(BadArgumentError):
            (
                RowLevelResultCheck(
                    level=CheckLevel.ERROR,
                    name="test",
                )
                .is_complete(id_columns=["id"], column="id")
                .is_complete(id_columns=["ID"], column="id")
                .is_complete(id_columns=["id"], column="ID")
            )

    @pytest.mark.parametrize("id_columns", [tuple(), list(), set()], ids=("tuple", "list", "set"))
    def test_is_complete_with_no_id_columns_specified(self, id_columns):
        with pytest.raises(BadArgumentError):
            RowLevelResultCheck(
                level=CheckLevel.ERROR,
                name="test",
            ).is_complete(id_columns=id_columns, column="id")

    @pytest.mark.parametrize(
        "value",
        [[1], [1, 2, 3]],
        ids=("list with len equal to 1", "list with len equal to 3"),
    )
    def test_is_between_with_invalid_value_len(self, value):
        with pytest.raises(BadArgumentError):
            (
                RowLevelResultCheck(
                    level=CheckLevel.ERROR,
                    name="test",
                ).is_between(column="id", value=value)
            )
