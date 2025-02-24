from typing import Any, Optional, Union

import pytest
from pydantic import ValidationError

from dataguard.store.audit.row import BaseAuditRow


@pytest.mark.unit
class TestBaseAuditRowUnit:
    @pytest.mark.parametrize(
        "field_type",
        [
            Any,
            list[Any],
            list[Any],
            set[Any],
            set[Any],
            tuple[Any],
            tuple[Any],
            Optional[Any],
        ],
    )
    def test_error_on_field_with_any_as_type(self, field_type):
        class AnyFieldAuditRow(BaseAuditRow):
            field: Any

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AnyFieldAuditRow(field="test")

    @pytest.mark.parametrize(
        "field_type",
        [
            int | float | str,
            Union[int, float, str],
            Optional[int | float | str],
        ],
    )
    def test_error_on_field_with_multi_type_scalars(self, field_type):
        class MultiTypeFieldAuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeFieldAuditRow(field="test")

    @pytest.mark.parametrize(
        "field_type",
        [
            list[int | float | str],
            list[int | float | str],
            list[int] | list[float],
            Union[list[int], list[float]],
            list,
            list,
            Optional[list[int | float | str]],
        ],
    )
    def test_error_on_list_with_multi_type_values(self, field_type):
        class MultiTypeListAuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeListAuditRow(field=[1, 2])

    @pytest.mark.parametrize(
        "field_type",
        [
            set[int | float | str],
            set[int | float | str],
            set[int] | set[float],
            Union[set[int], set[float]],
            set,
            set,
        ],
    )
    def test_error_on_set_with_multi_type_values(self, field_type):
        class MultiTypeSetAuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeSetAuditRow(field={1, 2})

    @pytest.mark.parametrize(
        "field_type",
        [
            tuple[int | float | str, ...],
            tuple[int | float | str],
            tuple[int | float | str],
            tuple[int] | tuple[float],
            Union[tuple[int], tuple[float]],
            tuple,
            tuple,
            Optional[tuple[int | float | str]],
            Optional[tuple[int | float | str, ...]],
        ],
    )
    def test_error_on_tuple_with_multi_type_values(self, field_type):
        class MultiTypeTupleAuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeTupleAuditRow(field=(1,))
