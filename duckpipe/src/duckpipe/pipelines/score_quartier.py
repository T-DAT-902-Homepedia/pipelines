"""Score quartier (gold) : écart qualité-prix à la maille IRIS.

MVP « gap quartier » : le prix est recalculé à l'IRIS (silver iris_prix,
médiane DVF poolée multi-millésimes) ; la qualité reste héritée de la commune
(score_territoire) — les dimensions recalculables à l'IRIS (transport, BPE,
DPE, revenus) viendront en phase 2.

`n_prix_iris` est normalisé par le même `_norm` que le `n_prix` communal
(clip p1-p99 + minmax), sur la population des IRIS fiables de France :
`gap_iris = score_commune - n_prix_iris` est sur la même échelle que le `gap`
communal et comparable entre communes. Nuance d'interprétation : n_prix_iris
est calé sur des prix poolés (annee_min..annee_max) là où n_prix l'est sur le
millésime courant — pour une commune mono-IRIS, gap_iris - gap mesure surtout
cet effet de fenêtre, pas un signal quartier.

INNER JOIN sur score_territoire : le gap exige le score hérité ; les IRIS de
communes non scorées (< 5 ventes au millésime courant) sont exclus du gold
même si leur fenêtre poolée les rend fiables.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline
from duckpipe.pipelines.score import _norm

if TYPE_CHECKING:
    import duckdb


def score_quartier(
    con: duckdb.DuckDBPyConnection,
    iris_prix: str,
    iris_geom: str,
    score_territoire: str,
) -> str:
    """Produit la table gold `score_quartier` (IRIS fiables de communes scorées).

    Le filtre commune scorée est appliqué AVANT `_norm` : la population de
    normalisation est exactement celle du gold, comme au grain communal où
    toute commune fiable est scorée (un IRIS fiable d'une commune non scorée
    n'influence pas les quantiles de clip).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE score_quartier AS
        WITH fiables AS (
            SELECT p.* FROM {iris_prix} p
            WHERE p.fiable
              AND p.code_commune IN (SELECT code_commune FROM {score_territoire})
        ),
        normalized AS (
            SELECT code_iris, code_commune, nb_transactions, prix_m2_median,
                   annee_min, annee_max, nb_millesimes,
                   {_norm("prix_m2_median")} AS n_prix_iris
            FROM fiables
        )
        SELECT
            n.code_iris,
            n.code_commune,
            g.nom_iris,
            g.type_iris,
            g.nb_iris_commune,
            s.nom_commune,
            s.code_departement,
            n.nb_transactions,
            n.prix_m2_median,
            n.annee_min,
            n.annee_max,
            n.nb_millesimes,
            n.n_prix_iris,
            s.score_valeur AS score_commune,
            s.n_prix AS n_prix_commune,
            s.n_access_fin AS n_access_fin_commune,
            s.score_valeur - n.n_prix_iris AS gap_iris,
            (s.score_valeur - n.n_prix_iris) * s.n_access_fin AS gap_pondere_iris
        FROM normalized n
        JOIN {iris_geom} g USING (code_iris)
        JOIN {score_territoire} s ON s.code_commune = n.code_commune
        """
    )
    return "score_quartier"


score_quartier_pipeline = Pipeline(
    nodes=[
        Node(
            func=score_quartier,
            inputs=["iris_prix", "iris_geom", "score_territoire"],
            outputs=["score_quartier"],
            name="score_quartier",
        ),
    ]
)
