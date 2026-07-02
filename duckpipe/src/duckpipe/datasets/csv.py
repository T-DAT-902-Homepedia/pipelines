from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from duckpipe.datasets.base import Dataset, DatasetError

if TYPE_CHECKING:
    import duckdb


class CsvDataset(Dataset):
    """Dataset CSV, local ou distant (tout chemin lisible par `read_csv` de DuckDB).

    `read_csv_kwargs` est injecté tel quel dans l'appel `read_csv(...)` DuckDB
    (ex. `{"compression": "gzip"}`) : ouvert à l'extension sans modifier cette
    classe (OCP).
    """

    def __init__(
        self,
        path: str,
        *,
        all_varchar: bool = True,
        read_csv_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.path = path
        self.all_varchar = all_varchar
        self.read_csv_kwargs = read_csv_kwargs or {}

    def load(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> str:
        from duckpipe import fetch  # noqa: PLC0415 — évite un cycle d'import au module

        options = {"header": True, "all_varchar": self.all_varchar, **self.read_csv_kwargs}
        options_sql = ", ".join(f"{key} = {_to_sql(value)}" for key, value in options.items())
        try:
            with fetch.local_read_path(self.path) as local_path:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS "
                    f"SELECT * FROM read_csv('{local_path}', {options_sql})"
                )
        except Exception as exc:
            raise DatasetError(f"Échec du chargement CSV depuis {self.path!r}") from exc
        return table_name

    def save(self, con: duckdb.DuckDBPyConnection, *, table_name: str) -> None:
        from duckpipe import fetch  # noqa: PLC0415

        try:
            with fetch.local_write_path(self.path) as local_path:
                con.execute(f"COPY {table_name} TO '{local_path}' (HEADER, DELIMITER ',')")
        except Exception as exc:
            raise DatasetError(f"Échec de l'écriture CSV vers {self.path!r}") from exc

    def exists(self, con: duckdb.DuckDBPyConnection) -> bool:
        if "://" in self.path:
            return False
        return Path(self.path).exists()


def _to_sql(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)
