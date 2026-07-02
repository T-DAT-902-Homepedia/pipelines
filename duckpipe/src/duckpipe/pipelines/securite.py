from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Seuil de plausibilité repris de exploration/src/preprocess.py : somme des
# taux/1000 sur ~14 indicateurs, un total > 2000 relève d'un artefact
# d'agrégation, pas d'une réalité communale.
TAUX_DELINQUANCE_MAX = 2_000


def securite(con: duckdb.DuckDBPyConnection, securite_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_securite`.

    Table `securite` : (code_commune, taux_delinquance_global,
    nb_indicateurs_diffuses, insee_pop) sur la dernière année disponible
    (base communale SSMSI, indicateurs effectivement diffusés uniquement).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE securite_brut AS
        WITH brut AS (
            SELECT
                lpad(CAST(CODGEO_2025 AS VARCHAR), 5, '0') AS code_commune,
                TRY_CAST(annee AS INTEGER) AS annee,
                est_diffuse,
                TRY_CAST(replace(taux_pour_mille, ',', '.') AS DOUBLE) AS taux,
                TRY_CAST(insee_pop AS INTEGER) AS insee_pop
            FROM {securite_raw}
            WHERE CODGEO_2025 IS NOT NULL
        ),
        derniere_annee AS (SELECT max(annee) AS a FROM brut)
        SELECT
            code_commune,
            sum(taux) AS taux_delinquance_global,
            count(*) AS nb_indicateurs_diffuses,
            max(insee_pop) AS insee_pop
        FROM brut, derniere_annee
        WHERE annee = derniere_annee.a
          AND est_diffuse = 'diff'
          AND taux IS NOT NULL
        GROUP BY code_commune
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE securite AS
        SELECT s.* FROM securite_brut s
        JOIN {commune_geom} g USING (code_commune)
        WHERE s.taux_delinquance_global BETWEEN 0 AND {TAUX_DELINQUANCE_MAX}
        """
    )
    return "securite"


securite_pipeline = Pipeline(
    nodes=[
        Node(
            func=securite,
            inputs=["securite_raw", "commune_geom"],
            outputs=["securite"],
            name="securite",
        ),
    ]
)
