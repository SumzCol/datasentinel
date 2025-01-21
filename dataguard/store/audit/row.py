from typing import Any

from pydantic import BaseModel


class BaseAuditRow(BaseModel):
    def to_dict(self) -> dict[str, Any]:
        """Returns the row as a dictionary."""
        return self.model_dump()

    def columns(self) -> list[str]:
        """Returns the columns of the row."""
        return list(self.model_fields.keys())