from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb


def equipements(con: duckdb.DuckDBPyConnection, equipements_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_equipements`.

    Table `equipements` : (code_commune, nb_services_sante, nb_loisirs_culture),
    agrégée de la BPE INSEE 2024 en 2 familles (A/B/C/D = services & santé,
    F = loisirs & culture). Les communes à 0 équipement sont conservées (0 est
    une information valide, pas un manquant).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE equipements_brut AS
        SELECT
            lpad(CAST(GEO AS VARCHAR), 5, '0') AS code_commune,
            coalesce(sum(TRY_CAST(OBS_VALUE AS DOUBLE))
                FILTER (WHERE FACILITY_DOM IN ('A','B','C','D')), 0) AS nb_services_sante,
            coalesce(sum(TRY_CAST(OBS_VALUE AS DOUBLE))
                FILTER (WHERE FACILITY_DOM = 'F'), 0) AS nb_loisirs_culture
        FROM {equipements_raw}
        WHERE GEO_OBJECT = 'COM' AND GEO IS NOT NULL
        GROUP BY code_commune
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE equipements AS
        SELECT e.* FROM equipements_brut e
        JOIN {commune_geom} g USING (code_commune)
        WHERE e.nb_services_sante >= 0 AND e.nb_loisirs_culture >= 0
        """
    )
    return "equipements"


equipements_pipeline = Pipeline(
    nodes=[
        Node(
            func=equipements,
            inputs=["equipements_raw", "commune_geom"],
            outputs=["equipements"],
            name="equipements",
        ),
    ]
)
