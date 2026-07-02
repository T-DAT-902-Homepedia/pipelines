from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset

if TYPE_CHECKING:
    import duckdb


@dataclass
class Catalog:
    """Registry nom logique -> Dataset.

    Le Pipeline/Node ne connaît que des noms (str) ; le mapping vers une
    implémentation concrète de Dataset (CSV local, Parquet GCS, ...) est
    entièrement décidé à la construction du Catalog, jamais dans le code des
    nodes (DIP + OCP : changer de format ou de backend de stockage ne touche
    pas les nodes).
    """

    _datasets: dict[str, Dataset] = field(default_factory=dict)

    def add(self, name: str, dataset: Dataset) -> Catalog:
        self._datasets[name] = dataset
        return self

    def load(self, con: duckdb.DuckDBPyConnection, name: str) -> str:
        """Charge le dataset `name` dans une table DuckDB du même nom."""
        return self._datasets[name].load(con, table_name=name)

    def save(self, con: duckdb.DuckDBPyConnection, name: str) -> None:
        """Persiste la table DuckDB `name` via le dataset enregistré sous ce nom."""
        self._datasets[name].save(con, table_name=name)
