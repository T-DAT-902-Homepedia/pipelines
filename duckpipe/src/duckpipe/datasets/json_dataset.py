from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset, DatasetError

if TYPE_CHECKING:
    import duckdb


class JsonDataset(Dataset):
    """Dataset JSON newline-delimited (JSONL), lu via `read_json_auto`.

    Écriture non supportée dans cette itération (source uniquement, ex. DPE).
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        from duckpipe import fetch  # noqa: PLC0415 — évite un cycle d'import au module

        try:
            with fetch.local_read_path(self.path) as local_path:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM read_json_auto('{local_path}', "
                    f"format = 'newline_delimited', ignore_errors = true)"
                )
        except Exception as exc:
            raise DatasetError(f"Échec du chargement JSON depuis {self.path!r}") from exc
        return table_name

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        raise DatasetError("JsonDataset ne supporte pas l'écriture (source uniquement).")

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        if "://" in self.path:
            return False
        return Path(self.path).exists()
