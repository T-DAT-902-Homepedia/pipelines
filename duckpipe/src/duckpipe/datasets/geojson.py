from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset, DatasetError

if TYPE_CHECKING:
    import duckdb


class GeoJsonDataset(Dataset):
    """Dataset GeoJSON, lu via `ST_Read` (extension `spatial` DuckDB).

    `ST_Read` passe par GDAL et non par `httpfs` : contrairement aux CSV et
    Parquet, un chemin `gs://` n'est pas lisible directement — on télécharge
    alors l'objet vers un fichier temporaire avant lecture.

    Écriture non supportée : les GeoJSON de ce pipeline sont uniquement des
    sources en entrée (contours communaux/départementaux), jamais des sorties.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        from duckpipe import fetch  # noqa: PLC0415 — évite un cycle d'import au module

        try:
            with fetch.local_read_path(self.path) as local_path:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM ST_Read('{local_path}')"
                )
        except Exception as exc:
            raise DatasetError(f"Échec du chargement GeoJSON depuis {self.path!r}") from exc
        return table_name

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        raise DatasetError("GeoJsonDataset ne supporte pas l'écriture (source uniquement).")

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        if "://" in self.path:
            return False
        return Path(self.path).exists()
