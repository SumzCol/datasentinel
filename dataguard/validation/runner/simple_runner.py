from datetime import datetime

from ulid import ULID

from dataguard.validation.data_validation import DataValidation
from dataguard.validation.result import DataValidationResult
from dataguard.validation.runner.core import (
    AbstractRunner,
)


class SimpleRunner(AbstractRunner):
    def _run(self, data_validation: DataValidation) -> DataValidationResult:
        data = data_validation.data_asset.load()
        start_time = datetime.now()
        check_results = [check.validate(data) for check in data_validation.check_list]
        end_time = datetime.now()

        return DataValidationResult(
            run_id=ULID(),
            name=data_validation.name,
            data_asset=data_validation.data_asset.name,
            data_asset_schema=data_validation.data_asset.schema,
            start_time=start_time,
            end_time=end_time,
            check_results=check_results,
        )
