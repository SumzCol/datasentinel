import csv
import json
import os

from dataguard.store.result.core import AbstractResultStore
from dataguard.validation.result import DataValidationResult


class CSVResultStore(AbstractResultStore):
    def __init__(self, name: str, filepath: str, bad_records_limit: int, disabled: bool = False):
        self._filepath = filepath
        self._bad_records_limit = bad_records_limit
        super().__init__(name=name, disabled=disabled)

    def store(self, result: DataValidationResult):
        fieldnames = [
            "run_id",
            "name",
            "data_asset",
            "data_asset_schema",
            "start_time",
            "end_time",
            "status",
            "check_name",
            "check_level",
            "check_class",
            "check_start_time",
            "check_end_time",
            "check_status",
            "rule_index",
            "rule_name",
            "rule_column",
            "rule_value",
            "rule_rows",
            "rule_violations",
            "rule_pass_rate",
            "rule_pass_threshold",
            "rule_options",
            "rule_failed_rows_dataset",
            "rule_status",
        ]
        validation_node_result_dict = result.to_dict()
        file_exists = os.path.isfile(self._filepath)
        with open(self._filepath, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for check_result in validation_node_result_dict.get("check_results"):
                for rule_metric in check_result.get("rule_metrics"):
                    writer.writerow(
                        {
                            "run_id": validation_node_result_dict["run_id"],
                            "name": validation_node_result_dict["name"],
                            "data_asset": validation_node_result_dict["data_asset"],
                            "data_asset_schema": validation_node_result_dict["data_asset_schema"],
                            "start_time": validation_node_result_dict["start_time"].isoformat(),
                            "end_time": validation_node_result_dict["end_time"].isoformat(),
                            "status": validation_node_result_dict["status"],
                            "check_name": check_result["name"],
                            "check_level": check_result["level"],
                            "check_class": check_result["check_class"],
                            "check_start_time": check_result["start_time"].isoformat(),
                            "check_end_time": check_result["end_time"].isoformat(),
                            "check_status": check_result["status"],
                            "rule_index": rule_metric["id"],
                            "rule_name": rule_metric["rule"],
                            "rule_column": rule_metric["column"],
                            "rule_value": rule_metric["value"],
                            "rule_rows": rule_metric["rows"],
                            "rule_violations": rule_metric["violations"],
                            "rule_pass_rate": rule_metric["pass_rate"],
                            "rule_pass_threshold": rule_metric["pass_threshold"],
                            "rule_options": (
                                json.dumps(rule_metric["options"])
                                if rule_metric["options"] is not None
                                else None
                            ),
                            "rule_failed_rows_dataset": (
                                rule_metric["failed_rows_dataset"].to_json(
                                    limit=self._bad_records_limit
                                )
                                if rule_metric["failed_rows_dataset"] is not None
                                else None
                            ),
                            "rule_status": rule_metric["status"],
                        }
                    )
