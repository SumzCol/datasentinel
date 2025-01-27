from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum
from types import UnionType, NoneType
from typing import Any, Tuple, Dict, get_args, get_origin, Union

from pydantic import BaseModel, model_validator
from typing_extensions import Self


_VALID_TYPES = [str, int, float, bool, datetime, date, dict, list, tuple, set]


@dataclass
class FieldInfo:
    annotation: type | None
    type: type
    required: bool
    complex: bool


class BaseAuditRow(BaseModel):
    @model_validator(mode="after")
    def validate_fields(self) -> Self:
        self._validate_fields()
        return self

    def to_dict(self) -> dict[str, Any]:
        """Returns the row as a dictionary."""
        return self.model_dump()

    def columns(self) -> list[str]:
        """Returns the columns of the row."""
        return list(self.model_fields.keys())

    @property
    def row_fields(self) -> Dict[str, FieldInfo]:
        """Returns the schema of the row."""
        return {
            name: FieldInfo(
                annotation=pydantic_field_info.annotation,
                type=self._field_type(pydantic_field_info.annotation),
                required=pydantic_field_info.is_required(),
                complex=self._is_complex(pydantic_field_info.annotation),
            )
            for name, pydantic_field_info in self.model_fields.items()
        }

    def _validate_fields(self) -> None:
        for name, pydantic_field_info in self.model_fields.items():
            if self._is_multi_type(pydantic_field_info.annotation):
                raise ValueError(f"Multi-type fields are not supported: {name}")

            _field_type = self._field_type(pydantic_field_info.annotation)
            if _field_type not in _VALID_TYPES:
                _str_types = ", ".join([t.__name__ for t in _VALID_TYPES])
                raise ValueError(
                    f"Field '{name}' has an invalid type '{_field_type}'. "
                    f"Valid types are: {_str_types}"
                )

    @staticmethod
    def _is_multi_type(annotation: type) -> bool:
        origin = get_origin(annotation)

        if not origin or (origin != Union and origin != UnionType):
            return False

        args = get_args(annotation)

        if len(args) == 1 or (len(args) == 2 and args[1] == NoneType):
            return False

        return True

    @staticmethod
    def _is_complex(annotation: type) -> bool:
        origin = get_origin(annotation)

        if not origin:
            return False

        if origin != Union and origin != UnionType:
            return True

        args = get_args(annotation)

        if len(args) == 1 or (len(args) == 2 and args[1] == NoneType):
            return False

        return True

    def _field_type(self, annotation: type) -> type:
        origin = get_origin(annotation)

        if not origin:
            return annotation

        if origin != Union and origin != UnionType:
            return origin

        args = get_args(annotation)

        if len(args) == 1 or (len(args) == 2 and args[1] == NoneType):
            if self._is_complex(args[0]):
                return self._field_type(args[0])
            return args[0]

        return annotation