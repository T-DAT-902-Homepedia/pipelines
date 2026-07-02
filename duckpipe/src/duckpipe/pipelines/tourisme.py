from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb


def tourisme(con: duckdb.DuckDBPyConnection, tourisme_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_tourisme`.

    Table `tourisme` : (code_commune, nb_logements, part_residences_secondaires),
    agrégée des IRIS à la commune (base infracommunale Logement INSEE 2021).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE tourisme_brut AS
        SELECT
            lpad(CAST(COM AS VARCHAR), 5, '0') AS code_commune,
            sum(TRY_CAST(P21_LOG AS DOUBLE)) AS nb_logements,
            sum(TRY_CAST(P21_RSECOCC AS DOUBLE))
                / NULLIF(sum(TRY_CAST(P21_LOG AS DOUBLE)), 0) AS part_residences_secondaires
        FROM {tourisme_raw}
        WHERE COM IS NOT NULL
        GROUP BY 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE tourisme AS
        SELECT t.* FROM tourisme_brut t
        JOIN {commune_geom} g USING (code_commune)
        WHERE t.nb_logements > 0
          AND t.part_residences_secondaires BETWEEN 0 AND 1
        """
    )
    return "tourisme"


tourisme_pipeline = Pipeline(
    nodes=[
        Node(
            func=tourisme,
            inputs=["tourisme_raw", "commune_geom"],
            outputs=["tourisme"],
            name="tourisme",
        ),
    ]
)
