from pydantic import ConfigDict, Field, field_validator
from pydantic.dataclasses import dataclass

from dataguard.validation.check.core import AbstractCheck
from dataguard.validation.core import NotifyOnEvent
from dataguard.validation.data_asset.core import AbstractDataAsset


@dataclass(frozen=True, config=ConfigDict(arbitrary_types_allowed=True))
class DataValidation:
    """Represent the parametrization of a data validation process.

    Attributes:
        name: The name of the data validation process.
        check_list: A list with the data quality checks to be applied to
            the data asset.
        data_asset: The data asset to be validated.
        result_stores: A list with the name of the result stores where the
            results of the validation process will be saved.
        notifiers_by_event: A dictionary where each key is an event, and the corresponding value
            is a list of the notifiers name to trigger when that event occurs.
    """

    name: str
    check_list: list[AbstractCheck]
    data_asset: AbstractDataAsset
    result_stores: list[str] = Field(default_factory=list)
    notifiers_by_event: dict[NotifyOnEvent, list[str]] = Field(default_factory=dict)

    @field_validator("check_list", mode="after")
    def validate_check_list(cls, check_list: list[AbstractCheck]) -> list[AbstractCheck]:
        if not check_list:
            raise ValueError("Data validation must have at least one check")
        return check_list

    @property
    def checks_count(self) -> int:
        return len(self.check_list)

    def check_exists(self, check_name: str) -> bool:
        return any(check.name == check_name for check in self.check_list)
