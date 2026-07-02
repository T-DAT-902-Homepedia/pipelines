from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Seuils de plausibilité repris de exploration/src/preprocess.py (revenu
# disponible médian communal, Filosofi). Réimplémentés inline (cf. décision
# de portage : pas d'import cross-repo entre pipelines/ et exploration/).
REVENU_MIN = 5_000
REVENU_MAX = 80_000


def revenus(con: duckdb.DuckDBPyConnection, revenus_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_revenus`.

    Table `revenus` : (code_commune, nom_commune, revenu_median, revenu_q1),
    revenu disponible médian par unité de consommation (INSEE Filosofi 2021).
    Rejette les communes masquées par le secret statistique (revenu NULL), hors
    bornes plausibles, ou orphelines du référentiel des contours.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE revenus_brut AS
        SELECT
            lpad(CAST("Code géographique" AS VARCHAR), 5, '0') AS code_commune,
            "Libellé géographique" AS nom_commune,
            TRY_CAST("[DISP] Médiane (€)" AS DOUBLE) AS revenu_median,
            TRY_CAST("[DISP] 1ᵉʳ quartile (€)" AS DOUBLE) AS revenu_q1
        FROM {revenus_raw}
        WHERE "Code géographique" IS NOT NULL
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE revenus AS
        SELECT r.* FROM revenus_brut r
        JOIN {commune_geom} g USING (code_commune)
        WHERE r.revenu_median IS NOT NULL
          AND r.revenu_median BETWEEN {REVENU_MIN} AND {REVENU_MAX}
        """
    )
    return "revenus"


revenus_pipeline = Pipeline(
    nodes=[
        Node(
            func=revenus,
            inputs=["revenus_raw", "commune_geom"],
            outputs=["revenus"],
            name="revenus",
        ),
    ]
)
