from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline
from duckpipe.pipelines.codes import dept_code_expr

if TYPE_CHECKING:
    import duckdb

# Seuils de plausibilité repris de exploration/src/preprocess.py::clean_dvf.
# Réimplémentés inline plutôt qu'importés cross-repo (pipelines/ et
# exploration/ sont deux dépôts frères, pas un monorepo à pyproject unique) ;
# à revisiter si un package partagé est extrait un jour.
PRIX_M2_MIN = 100
PRIX_M2_MAX = 50_000
SURFACE_MIN = 9
SURFACE_MAX = 1_000


def ingest_dvf(con: duckdb.DuckDBPyConnection, dvf_raw: str) -> str:
    """Adaptation de `exploration/src/ingest.py::ensure_dvf`.

    `dvf_raw` est déjà le nom de la table CSV brute chargée par le Catalog
    (le download est délégué au Dataset associé, plus géré ici). L'idempotence
    (skip si déjà ingérée) est déplacée au niveau infra/Catalog plutôt que
    testée dans le node.

    Le DVF éclate une vente en plusieurs lignes (une par lot/parcelle) : on
    déduplique par mutation avant de calculer le prix/m², sans quoi une
    mutation aberrante peut fausser toute une médiane communale.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dvf_lignes AS
        SELECT
            id_mutation,
            nature_mutation,
            type_local,
            TRY_CAST(valeur_fonciere AS DOUBLE) AS valeur_fonciere,
            TRY_CAST(surface_reelle_bati AS DOUBLE) AS surface_bati,
            lpad(CAST(code_commune AS VARCHAR), 5, '0') AS code_commune,
            nom_commune,
            {dept_code_expr("code_departement")} AS code_departement,
            TRY_CAST(longitude AS DOUBLE) AS longitude,
            TRY_CAST(latitude AS DOUBLE) AS latitude
        FROM {dvf_raw}
        WHERE nature_mutation = 'Vente'
          AND type_local IN ('Maison', 'Appartement')
          AND TRY_CAST(surface_reelle_bati AS DOUBLE) > 0
          AND TRY_CAST(valeur_fonciere AS DOUBLE) > 0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE dvf_dedup AS
        WITH par_mutation AS (
            SELECT id_mutation,
                   count(DISTINCT type_local) AS nb_types,
                   sum(surface_bati) AS surface_totale,
                   any_value(type_local) AS type_local,
                   any_value(valeur_fonciere) AS valeur_fonciere,
                   any_value(code_commune) AS code_commune,
                   any_value(nom_commune) AS nom_commune,
                   any_value(code_departement) AS code_departement,
                   any_value(longitude) AS longitude,
                   any_value(latitude) AS latitude
            FROM dvf_lignes
            GROUP BY id_mutation
            HAVING count(DISTINCT valeur_fonciere) = 1
               AND count(DISTINCT type_local) = 1
        )
        SELECT id_mutation, type_local, valeur_fonciere,
               surface_totale AS surface_bati,
               code_commune, nom_commune, code_departement, longitude, latitude,
               valeur_fonciere / NULLIF(surface_totale, 0) AS prix_m2
        FROM par_mutation
        """
    )

    # Bornes absolues de plausibilité (surface, coordonnées, prix/m²).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dvf_bornes AS
        SELECT * FROM dvf_dedup
        WHERE surface_bati BETWEEN {SURFACE_MIN} AND {SURFACE_MAX}
          AND longitude IS NOT NULL AND latitude IS NOT NULL
          AND prix_m2 BETWEEN {PRIX_M2_MIN} AND {PRIX_M2_MAX}
        """
    )

    # Clipping statistique fin par (département, type_local) aux quantiles 1-99 %.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE dvf_bounds AS
        SELECT code_departement, type_local,
               quantile_cont(prix_m2, 0.01) AS p_lo,
               quantile_cont(prix_m2, 0.99) AS p_hi
        FROM dvf_bornes GROUP BY code_departement, type_local
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE dvf AS
        SELECT c.*
        FROM dvf_bornes c
        JOIN dvf_bounds b USING (code_departement, type_local)
        WHERE c.prix_m2 BETWEEN b.p_lo AND b.p_hi
        """
    )
    return "dvf"


def commune_agg(con: duckdb.DuckDBPyConnection, dvf: str) -> str:
    """Adaptation de `exploration/src/ingest.py::ensure_commune_agg`.

    Marque (sans supprimer) les communes peu fiables (< 5 transactions).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE commune_agg AS
        SELECT
            code_commune,
            any_value(nom_commune) AS nom_commune,
            any_value(code_departement) AS code_departement,
            count(*) AS nb_transactions,
            median(prix_m2) AS prix_m2_median,
            quantile_cont(prix_m2, 0.25) AS prix_m2_p25,
            quantile_cont(prix_m2, 0.75) AS prix_m2_p75,
            avg(prix_m2) AS prix_m2_moyen,
            count(*) >= 5 AS fiable
        FROM {dvf}
        GROUP BY code_commune
        """
    )
    return "commune_agg"


def commune_agg_type(con: duckdb.DuckDBPyConnection, dvf: str) -> str:
    """Agrégats communaux ventilés par type de local (Maison/Appartement).

    Grain distinct de `commune_agg` (qui reste tous types confondus car il
    alimente le score) : sert le filtre type_local de la carte choroplèthe.
    Même seuil de fiabilité (>= 5 transactions au grain commune×type).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE commune_agg_type AS
        SELECT
            code_commune,
            type_local,
            any_value(nom_commune) AS nom_commune,
            any_value(code_departement) AS code_departement,
            count(*) AS nb_transactions,
            median(prix_m2) AS prix_m2_median,
            count(*) >= 5 AS fiable
        FROM {dvf}
        GROUP BY code_commune, type_local
        """
    )
    return "commune_agg_type"


def dept_agg(con: duckdb.DuckDBPyConnection, dvf: str) -> str:
    """Agrégats départementaux, tous types confondus ET par type de local.

    `type_local` vaut NULL sur les lignes tous-types (GROUPING SETS) : c'est
    la convention consommée par l'export choroplèthe départements.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE dept_agg AS
        SELECT
            code_departement,
            type_local,
            count(*) AS nb_transactions,
            median(prix_m2) AS prix_m2_median,
            count(*) >= 5 AS fiable
        FROM {dvf}
        GROUP BY GROUPING SETS ((code_departement), (code_departement, type_local))
        """
    )
    return "dept_agg"


dvf_pipeline = Pipeline(
    nodes=[
        Node(func=ingest_dvf, inputs=["dvf_raw"], outputs=["dvf"], name="ingest_dvf"),
        Node(func=commune_agg, inputs=["dvf"], outputs=["commune_agg"], name="commune_agg"),
        Node(
            func=commune_agg_type,
            inputs=["dvf"],
            outputs=["commune_agg_type"],
            name="commune_agg_type",
        ),
        Node(func=dept_agg, inputs=["dvf"], outputs=["dept_agg"], name="dept_agg"),
    ]
)
