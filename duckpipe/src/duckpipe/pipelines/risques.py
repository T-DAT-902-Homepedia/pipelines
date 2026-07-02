from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb


def risques(con: duckdb.DuckDBPyConnection, risques_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_risques`.

    Table `risques` : (code_commune, nb_arretes_catnat, nb_arretes_inondation),
    comptage des arrêtés de catastrophe naturelle par commune (base GASPAR).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE risques_brut AS
        SELECT
            lpad(CAST(code_commune AS VARCHAR), 5, '0') AS code_commune,
            count(*) AS nb_arretes_catnat,
            count(*) FILTER (WHERE lower(lib_risque_jo) LIKE '%inondation%'
                             OR lower(lib_risque_jo) LIKE '%coulée%') AS nb_arretes_inondation
        FROM {risques_raw}
        WHERE code_commune IS NOT NULL
        GROUP BY 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE risques AS
        SELECT r.* FROM risques_brut r
        JOIN {commune_geom} g USING (code_commune)
        WHERE r.nb_arretes_catnat >= 0
        """
    )
    return "risques"


risques_pipeline = Pipeline(
    nodes=[
        Node(
            func=risques,
            inputs=["risques_raw", "commune_geom"],
            outputs=["risques"],
            name="risques",
        ),
    ]
)
