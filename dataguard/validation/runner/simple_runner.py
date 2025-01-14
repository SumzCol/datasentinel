from datetime import datetime

from ulid import ULID

from dataguard.validation.data_asset.core import AbstractDataAsset
from dataguard.validation.node.result import ValidationNodeResult
from dataguard.validation.node.validation_node import ValidationNode
from dataguard.validation.runner.core import (
    AbstractRunner,
)


class SimpleRunner(AbstractRunner):
    def _run(
        self,
        validation_node: ValidationNode,
        data_asset: AbstractDataAsset
    ) -> ValidationNodeResult:
        start_time = datetime.now()
        data = data_asset.load()
        check_results = [
            check.evaluate(data)
            for check in validation_node.check_list
        ]
        end_time = datetime.now()

        return ValidationNodeResult(
            run_id=ULID(),
            name=validation_node.name,
            data_asset=data_asset.name,
            data_asset_schema=data_asset.schema,
            start_time=start_time,
            end_time=end_time,
            check_results=check_results,
            metadata=validation_node.metadata,
        )
