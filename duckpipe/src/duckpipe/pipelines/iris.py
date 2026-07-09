"""Maille quartier (IRIS INSEE) : contours silver et agrégat prix DVF.

CONTOURS-IRIS® (coédition IGN/INSEE) fournit ~49 400 IRIS France entière,
codés par ARRONDISSEMENT municipal pour Paris/Lyon/Marseille (751xx/6938x/
132xx) — la même convention que le DVF et score_territoire : le raccord est
un equi-join direct sur code_commune. Ne JAMAIS filtrer ces contours sur
commune_geom : les fichiers Etalab ne connaissent que les communes-mères
(75056…) et on perdrait les ~1 570 IRIS des arrondissements PLM.

L'agrégat prix (`iris_prix`) poole plusieurs millésimes DVF : au grain IRIS,
un seul millésime laisse trop d'IRIS sous le seuil de fiabilité (>= 5 ventes,
même convention que commune_agg). Médiane simple poolée, sans pondération de
récence — déterministe, symétrique de la chaîne communale ; la fenêtre est
exposée via annee_min/annee_max/nb_millesimes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


def iris_geom(con: duckdb.DuckDBPyConnection, iris_raw: str) -> str:
    """Contours IRIS silver (code_iris, code_commune, noms, type, geom).

    `nb_iris_commune` est matérialisé pour distinguer les communes mono-IRIS
    (affectation directe des mutations, export web filtré) des communes à
    vrais quartiers — plus robuste que `type_iris = 'Z'` si la nomenclature
    évolue.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE iris_geom AS
        SELECT
            code_iris,
            lpad(CAST(code_insee AS VARCHAR), 5, '0') AS code_commune,
            nom_commune,
            nom_iris,
            type_iris,
            count(*) OVER (PARTITION BY code_insee) AS nb_iris_commune,
            geom
        FROM {iris_raw}
        WHERE geom IS NOT NULL AND code_iris IS NOT NULL
        """
    )
    return "iris_geom"


iris_geom_pipeline = Pipeline(
    nodes=[
        Node(
            func=iris_geom, inputs=["iris_raw"], outputs=["iris_geom"], name="iris_geom"
        ),
    ]
)


def iris_prix(
    con: duckdb.DuckDBPyConnection,
    iris_geom: str,
    dvf: str,
    *,
    year: int,
    points_tables: dict[int, str],
) -> str:
    """Agrégat prix DVF au grain IRIS, sur la fenêtre poolée
    {year} ∪ points_tables (mutations géolocalisées des millésimes annexes).

    Affectation mutation -> IRIS en deux branches :
    - communes mono-IRIS (2/3 des IRIS) : equi-join sur le code commune, aucun
      test géométrique (l'IRIS EST la commune) ;
    - communes multi-IRIS : point-in-polygon CONTRAINT à la commune de la
      mutation (l'equi-join réduit les candidats à quelques IRIS par point et
      confine le bruit de géocodage frontalier dans la bonne commune).

    ST_Intersects (et non ST_Contains) : un point posé exactement sur une
    limite entre deux IRIS appartient aux deux — départagé ensuite de façon
    déterministe par le plus petit code_iris (même famille de choix que le
    départage alphabétique du DPE dominant, cf. score.py).
    """
    union_parts = [
        f"SELECT id_mutation, code_commune, longitude, latitude, prix_m2, "
        f"{year} AS annee FROM {dvf}"
    ]
    # id_mutation geo-dvf est préfixé par l'année : unique inter-millésimes.
    union_parts += [
        f"SELECT id_mutation, code_commune, longitude, latitude, prix_m2, "
        f"{annee} AS annee FROM {table}"
        for annee, table in sorted(points_tables.items())
    ]
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE iris_pool AS {' UNION ALL '.join(union_parts)}"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE iris_affectation AS
        SELECT * FROM (
            SELECT p.id_mutation, p.prix_m2, p.annee, i.code_iris, i.code_commune
            FROM iris_pool p
            JOIN {iris_geom} i
              ON i.code_commune = p.code_commune AND i.nb_iris_commune = 1
            UNION ALL
            SELECT p.id_mutation, p.prix_m2, p.annee, i.code_iris, i.code_commune
            FROM iris_pool p
            JOIN {iris_geom} i
              ON i.code_commune = p.code_commune AND i.nb_iris_commune > 1
             AND ST_Intersects(i.geom, ST_Point(p.longitude, p.latitude))
        )
        QUALIFY row_number() OVER (PARTITION BY id_mutation ORDER BY code_iris) = 1
        """
    )

    # Taux d'affectation : les pertes viennent des communes DVF absentes du
    # référentiel IRIS (décalage de millésimes COG) et des points hors de tout
    # IRIS de leur commune (géocodage) — surveillé en prod via ces logs.
    n_pool, n_millesimes = con.execute(
        "SELECT count(*), count(DISTINCT annee) FROM iris_pool"
    ).fetchone()
    n_affectees = con.execute("SELECT count(*) FROM iris_affectation").fetchone()[0]
    logger.info(
        "[iris_prix] %d mutations poolées (%d millésimes), %d affectées à un IRIS (%.2f %%)",
        n_pool,
        n_millesimes,
        n_affectees,
        100 * n_affectees / n_pool if n_pool else 0.0,
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE iris_prix AS
        SELECT
            code_iris,
            any_value(code_commune) AS code_commune,
            count(*) AS nb_transactions,
            median(prix_m2) AS prix_m2_median,
            count(*) >= 5 AS fiable,
            min(annee) AS annee_min,
            max(annee) AS annee_max,
            count(DISTINCT annee) AS nb_millesimes
        FROM iris_affectation
        GROUP BY code_iris
        """
    )
    return "iris_prix"


def make_iris_prix_pipeline(year: int, annees_points: list[int]) -> Pipeline:
    """Fabrique le Pipeline iris_prix pour l'année du run et les millésimes
    annexes effectivement disponibles (le CLI vérifie l'existence des
    dvf_points_<annee> : un millésime manquant réduit la fenêtre poolée au
    lieu d'échouer le run, comme l'évolution des fiches dans publish_web).
    """
    annees = sorted(set(annees_points) - {year})
    inputs = ["iris_geom", "dvf", *(f"dvf_points_{annee}" for annee in annees)]

    def _node_func(con: duckdb.DuckDBPyConnection, **tables: str) -> str:
        points_tables = {annee: tables[f"dvf_points_{annee}"] for annee in annees}
        return iris_prix(
            con,
            tables["iris_geom"],
            tables["dvf"],
            year=year,
            points_tables=points_tables,
        )

    return Pipeline(
        nodes=[
            Node(
                func=_node_func, inputs=inputs, outputs=["iris_prix"], name="iris_prix"
            ),
        ]
    )
