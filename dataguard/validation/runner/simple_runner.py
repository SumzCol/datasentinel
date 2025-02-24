from datetime import datetime

from ulid import ULID

from dataguard.validation.data_asset.core import AbstractDataAsset
from dataguard.validation.data_validation import DataValidation
from dataguard.validation.result import DataValidationResult
from dataguard.validation.runner.core import (
    AbstractRunner,
)


class SimpleRunner(AbstractRunner):
    def _run(
        self, data_validation: DataValidation, data_asset: AbstractDataAsset
    ) -> DataValidationResult:
        start_time = datetime.now()
        data = data_asset.load()
        check_results = [check.validate(data) for check in data_validation.check_list]
        end_time = datetime.now()

        return DataValidationResult(
            run_id=ULID(),
            name=data_validation.name,
            data_asset=data_asset.name,
            data_asset_schema=data_asset.schema,
            start_time=start_time,
            end_time=end_time,
            check_results=check_results,
        )
