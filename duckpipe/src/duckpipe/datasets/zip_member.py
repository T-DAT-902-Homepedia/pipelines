from __future__ import annotations

import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe.datasets.base import Dataset, DatasetError

if TYPE_CHECKING:
    import duckdb


class ZipMemberDataset(Dataset):
    """Extrait un membre d'une archive zip locale vers un fichier temporaire,
    puis délègue le chargement à `inner` (typiquement CsvDataset).

    Composition plutôt qu'héritage (OCP) : ajoute le support ZIP à n'importe
    quel Dataset existant sans le modifier. `inner.path` est le chemin de
    destination de l'extraction ; `zip_path` est l'archive source.
    """

    def __init__(self, zip_path: str, member: str, inner: Dataset) -> None:
        self.zip_path = zip_path
        self.member = member
        self.inner = inner

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        extracted_path = Path(self.inner.path)  # type: ignore[attr-defined]
        if not extracted_path.exists():
            extracted_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with zipfile.ZipFile(self.zip_path) as archive, archive.open(self.member) as src:
                    extracted_path.write_bytes(src.read())
            except Exception as exc:
                raise DatasetError(
                    f"Échec de l'extraction de {self.member!r} depuis {self.zip_path!r}"
                ) from exc
        return self.inner.load(con, table_name=table_name)

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        raise DatasetError("ZipMemberDataset ne supporte pas l'écriture (source uniquement).")

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        return Path(self.inner.path).exists()  # type: ignore[attr-defined]
