from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


class DatasetError(RuntimeError):
    """Erreur de chargement/sauvegarde d'un Dataset (fichier absent, format invalide, etc.)."""


class Dataset(ABC):
    """Interface commune de persistance : encapsule où et comment une source ou
    destination de données est lue ou écrite, indépendamment de la logique
    métier des nodes.

    Contrat LSP : toute implémentation doit être substituable à une autre sans
    que l'appelant (Node/Pipeline) n'ait à changer de comportement — load()
    renvoie toujours le nom de la table DuckDB matérialisée, save() ne prend
    toujours qu'un nom de table source.
    """

    @abstractmethod
    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        """Matérialise la donnée source dans `table_name` et renvoie ce nom.

        Idempotent : un load répété écrase proprement la table.
        """

    @abstractmethod
    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        """Persiste le contenu de la table DuckDB `table_name` vers la destination."""

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        """Best-effort : par défaut False (force le load), surchargeable."""
        return False
