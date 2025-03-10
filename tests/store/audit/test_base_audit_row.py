from datetime import date, datetime
from enum import Enum
from typing import Any, Optional, Union

import pytest
from pydantic import ValidationError

from dataguard.store.audit.row import BaseAuditRow


@pytest.mark.unit
class TestBaseAuditRowUnit:
    @pytest.mark.parametrize(
        "field_type, field_value",
        [
            (Any, "test"),
            (list[Any], ["test"]),
            (set[Any], {"test"}),
            (tuple[Any], ("test",)),
            (Optional[Any], None),
        ],
    )
    def test_error_on_field_with_any_as_type(self, field_type, field_value):
        class AuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field=field_value)

    @pytest.mark.parametrize(
        "field_type",
        [
            int | float | str,
            Union[int, float, str],
            Optional[int | float | str],
        ],
    )
    def test_error_on_field_with_multi_type_scalars(self, field_type):
        class AuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field="test")

    @pytest.mark.parametrize(
        "field_type",
        [
            list[int | float | str],
            list[int] | list[float],
            list[int, float, str],
            Union[list[int], list[float]],
            list,
            Optional[list[int | float | str]],
        ],
    )
    def test_error_on_list_with_multi_type_values(self, field_type):
        class AuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field=[1, 2])

    @pytest.mark.parametrize(
        "field_type",
        [
            set[int | float | str],
            set[int] | set[float],
            set[int, float, str],
            Union[set[int], set[float]],
            set,
            Optional[set[int | float | str]],
        ],
    )
    def test_error_on_set_with_multi_type_values(self, field_type):
        class AuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field={1, 2})

    @pytest.mark.parametrize(
        "field_type, value",
        [
            (tuple[int | float | str, ...], (1,)),
            (tuple[int | float | str], (1,)),
            (tuple[int] | tuple[float], (1,)),
            (tuple[int, str], (1, "1")),
            (Union[tuple[int], tuple[float]], (1,)),
            (tuple, (1,)),
            (Optional[tuple[int | float | str]], (1,)),
            (Optional[tuple[int | float | str, ...]], (1,)),
        ],
    )
    def test_error_on_tuple_with_multi_type_values(self, field_type, value):
        class AuditRow(BaseAuditRow):
            field: field_type

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field=value)

    def test_error_on_union_as_optional_field(self):
        class AuditRow(BaseAuditRow):
            field: Optional[Union[int, str]]

        with pytest.raises(ValidationError, match="Multi-type fields are not supported"):
            AuditRow(field="test")

    def test_error_on_enum_field(self):
        class TestEnum(str, Enum):
            A = "a"
            B = "b"

        class AuditRow(BaseAuditRow):
            field: TestEnum

        with pytest.raises(ValidationError, match="Unsupported field type"):
            AuditRow(field=TestEnum.A)

    @pytest.mark.parametrize(
        "field_type, field_value, expected_type",
        [
            (int, 1, int),
            (int | None, 1, int),
            (Optional[int], 1, int),
            (str, "test", str),
            (str | None, "test", str),
            (Optional[str], "test", str),
            (float, 1.0, float),
            (float | None, 1.0, float),
            (Optional[float], 1.0, float),
            (bool, True, bool),
            (bool | None, True, bool),
            (Optional[bool], True, bool),
            (datetime, datetime.now(), datetime),
            (datetime | None, datetime.now(), datetime),
            (Optional[datetime], datetime.now(), datetime),
            (date, date.today(), date),
            (date | None, date.today(), date),
            (Optional[date], date.today(), date),
        ],
    )
    def test_row_fields_output_with_scalar_field(self, field_type, field_value, expected_type):
        class AuditRow(BaseAuditRow):
            field: field_type

        result = AuditRow(field=field_value).row_fields.get("field")
        assert result.type == expected_type
        assert not result.complex
        assert result.required
        assert result.args is None

    @pytest.mark.parametrize(
        "field_type, field_value, expected_args",
        [
            (list[int], [1], (int,)),
            (list[str], ["a"], (str,)),
            (list[float], [1.0], (float,)),
            (list[bool], [True], (bool,)),
            (list[datetime], [datetime.now()], (datetime,)),
            (list[date], [date.today()], (date,)),
            (Optional[list[int]], [1], (int,)),
            (list[int] | None, [1], (int,)),
        ],
    )
    def test_row_fields_output_with_list_field(self, field_type, field_value, expected_args):
        class AuditRow(BaseAuditRow):
            field: field_type

        result = AuditRow(field=field_value).row_fields.get("field")
        assert result.type is list
        assert result.args == expected_args
        assert result.complex
        assert result.required

    @pytest.mark.parametrize(
        "field_type, field_value, expected_args",
        [
            (set[int], {1}, (int,)),
            (set[str], {"a"}, (str,)),
            (set[float], {1.0}, (float,)),
            (set[bool], {True}, (bool,)),
            (set[datetime], {datetime.now()}, (datetime,)),
            (set[date], {date.today()}, (date,)),
            (Optional[set[int]], {1}, (int,)),
            (set[int] | None, {1}, (int,)),
        ],
    )
    def test_row_fields_output_with_set_field(self, field_type, field_value, expected_args):
        class AuditRow(BaseAuditRow):
            field: field_type

        result = AuditRow(field=field_value).row_fields.get("field")
        assert result.type is set
        assert result.args == expected_args
        assert result.complex
        assert result.required

    @pytest.mark.parametrize(
        "field_type, field_value, expected_args",
        [
            (tuple[int], (1,), (int,)),
            (tuple[int, ...], (1, 1), (int,)),
            (tuple[str], ("a",), (str,)),
            (tuple[float], (1.0,), (float,)),
            (tuple[bool], (True,), (bool,)),
            (tuple[datetime], (datetime.now(),), (datetime,)),
            (tuple[date], (date.today(),), (date,)),
            (Optional[tuple[int]], (1,), (int,)),
            (tuple[int] | None, (1,), (int,)),
        ],
    )
    def test_row_fields_output_with_tuple_field(self, field_type, field_value, expected_args):
        class AuditRow(BaseAuditRow):
            field: field_type

        result = AuditRow(field=field_value).row_fields.get("field")
        assert result.type is tuple
        assert result.args == expected_args
        assert result.complex
        assert result.required

    @pytest.mark.parametrize(
        "field_type, field_value, expected_args",
        [
            (dict, {"a": 1}, tuple()),
            (dict[str, int], {"a": 1}, (str, int)),
            (dict[str, dict[str, int]], {"a": {"b": 1}}, (str, dict[str, int])),
            (Optional[dict[str, int]], {"a": 1}, (str, int)),
            (dict[str, int] | None, {"a": 1}, (str, int)),
        ],
    )
    def test_row_fields_output_with_dict_field(self, field_type, field_value, expected_args):
        class AuditRow(BaseAuditRow):
            field: field_type

        result = AuditRow(field=field_value).row_fields.get("field")
        assert result.type is dict
        assert result.args == expected_args
        assert result.complex
        assert result.required

    def test_columns(self):
        class AuditRow(BaseAuditRow):
            field1: int
            field2: str

        audit_row = AuditRow(field1=1, field2="test")
        assert audit_row.columns == ["field1", "field2"]

    def test_to_dict(self):
        class AuditRow(BaseAuditRow):
            field1: int
            field2: str

        audit_row = AuditRow(field1=1, field2="test")
        assert audit_row.to_dict() == {"field1": 1, "field2": "test"}
