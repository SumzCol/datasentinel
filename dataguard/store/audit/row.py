from dataclasses import dataclass
from datetime import datetime, date
from types import UnionType, NoneType
from typing import Any, Dict, get_args, get_origin, Union

from pydantic import BaseModel, model_validator
from typing_extensions import Self

_VALID_SCALAR_TYPES = [str, int, float, bool, datetime, date]
_VALID_COLLECTION_TYPES = [list, tuple, set]
_VALID_TYPES = [*_VALID_SCALAR_TYPES, *_VALID_COLLECTION_TYPES, dict]


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
                raise ValueError(f"Multi-type fields are not supported '{name}'.")

            # _field_type = self._field_type(pydantic_field_info.annotation)
            # if _field_type not in _VALID_TYPES:
            #     _str_types = ", ".join([t.__name__ for t in _VALID_TYPES])
            #     raise ValueError(
            #         f"Field '{name}' has an invalid type '{_field_type}'. "
            #         f"Valid types are: {_str_types}"
            #     )

    def _is_multi_type(self, annotation: type) -> bool:
        field_type = get_origin(annotation) or annotation
        args = get_args(annotation)

        if field_type in _VALID_SCALAR_TYPES:
            return False

        if field_type is Any:
            return True

        if field_type is dict:
            return False

        if not (
            self._is_multi_typed_collection(field_type=field_type, args=args)
            or self._is_multi_typed_optional(field_type=field_type, args=args)
        ):
            return False

        return True

    def _is_multi_typed_optional(self, field_type: type, args: tuple | None) -> bool:
        if not field_type == Union and not field_type == UnionType:
            return False

        if len(args) == 2 and args[1] == NoneType and not self._is_multi_type(args[0]):
            return False

        return True

    @staticmethod
    def _is_multi_typed_collection(field_type: type, args: tuple | None) -> bool:
        if field_type not in _VALID_COLLECTION_TYPES:
            return False
        if not args:
            return True

        if field_type is list and len(args) == 1 and args[0] in _VALID_SCALAR_TYPES:
            return False

        if field_type is tuple and (
            (len(args) == 1 and args[0] in _VALID_SCALAR_TYPES)
            or (
                len(args) == 2
                and args[0] in _VALID_SCALAR_TYPES
                and args[1] == Ellipsis
            )
        ):
            return False

        if field_type is set and len(args) == 1 and args[0] in _VALID_SCALAR_TYPES:
            return False

        return True

    @staticmethod
    def _is_complex(annotation: type) -> bool:
        origin = get_origin(annotation)
        args = get_args(annotation)

        if annotation in _VALID_SCALAR_TYPES or not origin:
            return False

        if (origin == Union or origin == UnionType) and (
            args and len(args) == 2 and args[1] == NoneType
        ):
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
