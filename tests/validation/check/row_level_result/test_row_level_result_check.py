import pytest

from dataguard.validation.check import RowLevelResultCheck
from dataguard.validation.check.core import BadArgumentError


@pytest.mark.unit
class TestRowLevelResultCheckUnit:
    def test_is_complete_id_columns_in_evaluated_columns(self, check: RowLevelResultCheck):
        with pytest.raises(BadArgumentError):
            (
                check.is_complete(id_columns=["id"], column="id")
                .is_complete(id_columns=["ID"], column="id")
                .is_complete(id_columns=["id"], column="ID")
            )

    @pytest.mark.parametrize("id_columns", [tuple(), list(), set()], ids=("tuple", "list", "set"))
    def test_error_on_is_complete_with_no_id_columns_specified(
        self, check: RowLevelResultCheck, id_columns
    ):
        with pytest.raises(BadArgumentError):
            check.is_complete(id_columns=id_columns, column="id")

    def test_error_on_is_custom_with_no_callable_specified(self, check: RowLevelResultCheck):
        with pytest.raises(BadArgumentError):
            check.is_custom(fn=1)

    def test_error_on_is_custom_with_invalid_function_parameters(
        self, check: RowLevelResultCheck
    ):
        def invalid_fn(arg1, arg2, arg3):
            pass

        def invalid_fn2():
            pass

        with pytest.raises(BadArgumentError):
            check.is_custom(fn=invalid_fn)
        with pytest.raises(BadArgumentError):
            check.is_custom(fn=invalid_fn2)
