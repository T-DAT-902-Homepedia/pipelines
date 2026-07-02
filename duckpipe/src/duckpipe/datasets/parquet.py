from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset, DatasetError

if TYPE_CHECKING:
    import duckdb


class ParquetDataset(Dataset):
    """Dataset Parquet, local ou GCS (`gs://...`).

    Une seule implémentation sert les tests (path local) et la prod (gs://) :
    les chemins GCS transitent par un fichier temporaire via
    `fetch.local_read_path`/`local_write_path` (authentification OAuth sans
    clé statique — les clés HMAC qu'exigerait l'accès gs:// natif de DuckDB
    sont interdites par la politique d'organisation).
    """

    def __init__(self, path: str, *, partition_by: list[str] | None = None) -> None:
        self.path = path
        self.partition_by = partition_by
        if partition_by and "://" in path:
            raise DatasetError(
                "partition_by n'est pas supporté sur un chemin distant "
                "(l'upload d'une arborescence partitionnée n'est pas implémenté)"
            )

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        from duckpipe import fetch  # noqa: PLC0415 — évite un cycle d'import au module

        try:
            with fetch.local_read_path(self.path) as local_path:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM read_parquet('{local_path}')"
                )
        except Exception as exc:
            raise DatasetError(f"Échec du chargement Parquet depuis {self.path!r}") from exc
        return table_name

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        from duckpipe import fetch  # noqa: PLC0415

        options = "FORMAT PARQUET"
        if self.partition_by:
            columns = ", ".join(self.partition_by)
            options += f", PARTITION_BY ({columns})"
        try:
            with fetch.local_write_path(self.path) as local_path:
                con.execute(f"COPY {table_name} TO '{local_path}' ({options})")
        except Exception as exc:
            raise DatasetError(f"Échec de l'écriture Parquet vers {self.path!r}") from exc

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        if "://" in self.path:
            return False
        return Path(self.path).exists()
