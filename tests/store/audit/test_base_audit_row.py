from typing import List, Any, Union, Optional, Set, Tuple

import pytest
from pydantic import ValidationError

from dataguard.store.audit.row import BaseAuditRow


@pytest.mark.unit
class TestBaseAuditRowUnit:

    @pytest.mark.parametrize(
        "field_type",
        [
            Any,
            List[Any],
            list[Any],
            Set[Any],
            set[Any],
            Tuple[Any],
            tuple[Any],
            Optional[Any],
        ]
    )
    def test_error_on_field_with_any_as_type(self, field_type):
        class MultiTypeFieldAuditRow(BaseAuditRow):
            field: Any

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeFieldAuditRow(field="test")

    @pytest.mark.parametrize(
        "field_type",
        [
            List[int|float|str],
            List[Union[int, float, str]],
            List[int] | List[float],
            Union[List[int], List[float]],
            List,
            list,
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
            Set[int|float|str],
            Set[Union[int, float, str]],
            Set[int] | Set[float],
            Union[Set[int], Set[float]],
            Set,
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
            Tuple[int|float|str, ...],
            Tuple[Union[int, float, str]],
            Tuple[int] | Tuple[float],
            Union[Tuple[int], Tuple[float]],
            tuple,
            Tuple,
        ],
    )
    def test_error_on_tuple_with_multi_type_values(self, field_type):
        class MultiTypeTupleAuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            MultiTypeTupleAuditRow(field=(1,))