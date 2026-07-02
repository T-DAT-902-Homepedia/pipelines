from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

    from duckpipe.catalog import Catalog
    from duckpipe.node import Pipeline


def run_pipeline(con: duckdb.DuckDBPyConnection, pipeline: Pipeline, catalog: Catalog) -> None:
    """Exécute `pipeline` sur `con` en résolvant les Dataset via `catalog`."""
    pipeline.run(con, catalog)
