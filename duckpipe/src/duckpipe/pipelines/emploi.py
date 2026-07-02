from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Seuils de plausibilité repris de exploration/src/preprocess.py.
CHOMAGE_MAX = 50.0
COUVERTURE_MAX = 50.0


def emploi(con: duckdb.DuckDBPyConnection, emploi_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_emploi`.

    Table `emploi` : (code_commune, pop_active, taux_chomage,
    taux_couverture_emploi) — base communale RP 2021 INSEE.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE emploi_brut AS
        SELECT
            lpad(CAST(CODGEO AS VARCHAR), 5, '0') AS code_commune,
            TRY_CAST(P21_ACT1564 AS INTEGER) AS pop_active,
            CASE
                WHEN TRY_CAST(P21_ACT1564 AS DOUBLE) > 0
                THEN ROUND(TRY_CAST(P21_CHOM1564 AS DOUBLE)
                     / TRY_CAST(P21_ACT1564 AS DOUBLE) * 100, 2)
            END AS taux_chomage,
            CASE
                WHEN TRY_CAST(P21_ACT1564 AS DOUBLE) > 0
                THEN ROUND(TRY_CAST(P21_EMPLT AS DOUBLE)
                     / TRY_CAST(P21_ACT1564 AS DOUBLE), 3)
            END AS taux_couverture_emploi
        FROM {emploi_raw}
        WHERE CODGEO IS NOT NULL
          AND TRY_CAST(P21_ACT1564 AS DOUBLE) > 0
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE emploi AS
        SELECT e.* FROM emploi_brut e
        JOIN {commune_geom} g USING (code_commune)
        WHERE e.taux_chomage BETWEEN 0 AND {CHOMAGE_MAX}
          AND e.taux_couverture_emploi BETWEEN 0 AND {COUVERTURE_MAX}
        """
    )
    return "emploi"


emploi_pipeline = Pipeline(
    nodes=[
        Node(
            func=emploi,
            inputs=["emploi_raw", "commune_geom"],
            outputs=["emploi"],
            name="emploi",
        ),
    ]
)
