from datetime import datetime

from ulid import ULID

from dataguard.validation.datasource.core import AbstractDatasource
from dataguard.validation.node.result import ValidationNodeResult
from dataguard.validation.node.validation_node import ValidationNode
from dataguard.validation.runner.core import (
    AbstractRunner,
)


class SimpleRunner(AbstractRunner):
    def _run(
        self,
        validation_node: ValidationNode,
        datasource: AbstractDatasource
    ) -> ValidationNodeResult:
        start_time = datetime.now()
        data = datasource.load()
        check_results = [
            check.evaluate(data)
            for check in validation_node.check_list
        ]
        end_time = datetime.now()

        return ValidationNodeResult(
            run_id=ULID(),
            name=validation_node.name,
            data_asset=datasource.data_asset,
            data_asset_schema=datasource.data_asset_schema,
            start_time=start_time,
            end_time=end_time,
            check_results=check_results,
            metadata=validation_node.metadata,
        )
