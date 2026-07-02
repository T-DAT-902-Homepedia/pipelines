from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset

if TYPE_CHECKING:
    import duckdb


class MemoryDataset(Dataset):
    """Dataset no-op : la table reste en base DuckDB, jamais persistée sur disque.

    Utile pour les tables intermédiaires inter-nodes qui n'ont pas vocation à
    être écrites en silver/gold (ex. géométries lourdes en colonne GEOMETRY,
    non exportables tel quel en CSV/Parquet plat), et en tests.
    """

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        return table_name

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        return None
