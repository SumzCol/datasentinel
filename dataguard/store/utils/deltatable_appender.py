from copy import deepcopy
from typing import Literal, Dict, Any

from pyspark.sql import DataFrame

from dataguard.core import DataGuardError


class DeltaTableAppenderError(DataGuardError):
    pass


class DeltaTableAppender:
    def __init__(
        self,
        table: str,
        schema: str,
        dataset_type: Literal["file", "table"],
        external_path: str = None,
        save_args: Dict[str, Any] = None,
    ):
        if dataset_type not in ["file", "table"]:
            raise DeltaTableAppenderError(
                f"Invalid dataset type: {dataset_type}. Valid types are 'file' and 'table'"
            )
        self._dataset_type = dataset_type
        self._table = table
        self._schema = schema.rstrip("/")

        if dataset_type == "table":
            if not len(schema.split(".")) == 2:
                raise DeltaTableAppenderError(
                    f"Invalid table schema: {schema}. "
                    "It must be in the format 'catalog.schema'"
                )
            self._full_table_path = f"{schema}.{table}"

            if external_path:
                self._external_path = external_path
                self._is_external_table = True
            else:
                self._is_external_table = False
        else:
            self._full_table_path = f"{schema}/{table}"
            self._is_external_table = False

        self._save_args = deepcopy(save_args) if save_args else {}

        if "mode" in self._save_args:
            del self._save_args["mode"]

        if "format" in self._save_args:
            del self._save_args["format"]

    def _save_as_file(self, df: DataFrame):
        df.write.save(
            path=self._full_table_path, format="delta", mode="append", **self._save_args
        )

    def _save_as_table(self, df: DataFrame):
        _options = {
            "name": self._full_table_path,
            "mode": "append",
            "format": "delta",
            **self._save_args,
        }

        if self._is_external_table:
            _options["path"] = self._external_path

        df.write.saveAsTable(**_options)

    def append(self, df: DataFrame):
        if self._dataset_type == "file":
            self._save_as_file(df)
        else:
            self._save_as_table(df)
