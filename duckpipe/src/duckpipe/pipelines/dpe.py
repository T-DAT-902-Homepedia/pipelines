from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb


def dpe(con: duckdb.DuckDBPyConnection, dpe_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_dpe`.

    Table `dpe` : (code_commune, etiquette_dpe), échantillon national ADEME.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dpe_brut AS
        SELECT lpad(CAST(code_insee_ban AS VARCHAR), 5, '0') AS code_commune,
               upper(trim(etiquette_dpe)) AS etiquette_dpe
        FROM {dpe_raw}
        WHERE etiquette_dpe IS NOT NULL
          AND upper(trim(etiquette_dpe)) IN ('A','B','C','D','E','F','G')
          AND code_insee_ban IS NOT NULL
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE dpe AS
        SELECT d.* FROM dpe_brut d
        JOIN {commune_geom} g USING (code_commune)
        WHERE d.etiquette_dpe IN ('A','B','C','D','E','F','G')
        """
    )
    return "dpe"


dpe_pipeline = Pipeline(
    nodes=[
        Node(func=dpe, inputs=["dpe_raw", "commune_geom"], outputs=["dpe"], name="dpe"),
    ]
)
