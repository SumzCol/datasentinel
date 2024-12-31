import csv
import os
from typing import List

from dataguard.store.audit.core import AbstractAuditStore, AuditRow, AuditStoreError


class CSVAuditStore(AbstractAuditStore):
    def __init__(
            self,
            name: str,
            filepath: str,
            delimiter: str = ',',
            disabled: bool = False
    ) -> None:
        self._filepath = filepath
        self._delimiter = delimiter
        super().__init__(name, disabled)

    def _exists(self) -> bool:
        return os.path.exists(self._filepath)

    def _get_headers(self) -> List[str]:
        headers = []

        if not self._exists():
            return headers

        with open(self._filepath, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self._delimiter)
            for row in csv_reader:
                headers = row
                break

        return headers

    @staticmethod
    def _validate_columns(row_headers: List[str], store_headers: List[str]) -> None:
        if not store_headers:
            return

        if not (
            list(map(lambda x: x.lower(), store_headers)) ==
            list(map(lambda x: x.lower(), row_headers))
        ):
            raise AuditStoreError(
                f'The columns of the row to be appended ({row_headers}) dont match the '
                f'columns of rows stored in the audit store ({store_headers}).'
            )

    def _append_row(self, row: AuditRow, first_write: bool) -> None:
        mode = 'w' if first_write else 'a'
        with open(self._filepath, mode) as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=row.columns)
            if first_write:
                writer.writeheader()
            writer.writerow(row.to_dict())

    def append(self, row: AuditRow):
        headers = self._get_headers()
        self._validate_columns(row_headers=row.columns, store_headers=headers)
        self._append_row(row, first_write=not headers)
