from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline
from duckpipe.pipelines.codes import dept_code_expr
from duckpipe.pipelines.dvf import PRIX_M2_MAX, PRIX_M2_MIN, SURFACE_MAX, SURFACE_MIN

if TYPE_CHECKING:
    import duckdb


def prix_millesime(con: duckdb.DuckDBPyConnection, dvf_millesime_raw: str) -> dict[str, str]:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_prix_millesime`.

    Deux sorties depuis la même chaîne DVF (dédup par mutation + nettoyage de
    plausibilité + clipping p1-p99) appliquée à un millésime arbitraire :
    - `commune_prix` : (code_commune, nb_transactions, prix_m2_median) —
      agrégat communal léger pour l'analyse temporelle ;
    - `dvf_points` : mutations géolocalisées au grain unitaire, consommées par
      l'agrégat quartier (iris_prix poole plusieurs millésimes pour passer le
      seuil de fiabilité au grain IRIS).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE millesime_lignes AS
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
        FROM {dvf_millesime_raw}
        WHERE nature_mutation = 'Vente'
          AND type_local IN ('Maison', 'Appartement')
          AND TRY_CAST(surface_reelle_bati AS DOUBLE) > 0
          AND TRY_CAST(valeur_fonciere AS DOUBLE) > 0
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE millesime_dedup AS
        WITH par_mutation AS (
            SELECT id_mutation,
                   count(DISTINCT type_local) AS nb_types,
                   sum(surface_bati) AS surface_totale,
                   any_value(type_local) AS type_local,
                   any_value(valeur_fonciere) AS valeur_fonciere,
                   any_value(code_commune) AS code_commune,
                   any_value(code_departement) AS code_departement,
                   any_value(longitude) AS longitude,
                   any_value(latitude) AS latitude
            FROM millesime_lignes
            GROUP BY id_mutation
            HAVING count(DISTINCT valeur_fonciere) = 1
               AND count(DISTINCT type_local) = 1
        )
        SELECT id_mutation, type_local, valeur_fonciere,
               surface_totale AS surface_bati, code_commune, code_departement,
               longitude, latitude,
               valeur_fonciere / NULLIF(surface_totale, 0) AS prix_m2
        FROM par_mutation
        """
    )
    # Bornes absolues identiques à preprocess.clean_dvf (surface, coordonnées
    # présentes, prix/m² plausible) : l'original délègue ce filtre à clean_dvf,
    # ici réimplémenté inline (cf. décision de portage, pas d'import cross-repo).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE millesime_bornes AS
        SELECT * FROM millesime_dedup
        WHERE surface_bati BETWEEN {SURFACE_MIN} AND {SURFACE_MAX}
          AND longitude IS NOT NULL AND latitude IS NOT NULL
          AND prix_m2 BETWEEN {PRIX_M2_MIN} AND {PRIX_M2_MAX}
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE millesime_bounds AS
        SELECT code_departement, type_local,
               quantile_cont(prix_m2, 0.01) AS p_lo,
               quantile_cont(prix_m2, 0.99) AS p_hi
        FROM millesime_bornes GROUP BY code_departement, type_local
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE millesime_clean AS
        SELECT c.*
        FROM millesime_bornes c
        JOIN millesime_bounds b USING (code_departement, type_local)
        WHERE c.prix_m2 BETWEEN b.p_lo AND b.p_hi
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE dvf_points AS
        SELECT id_mutation, code_commune, longitude, latitude, prix_m2, type_local
        FROM millesime_clean
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE commune_prix AS
        SELECT code_commune,
               count(*) AS nb_transactions,
               median(prix_m2) AS prix_m2_median
        FROM millesime_clean
        GROUP BY code_commune
        """
    )
    return {"commune_prix": "commune_prix", "dvf_points": "dvf_points"}


def make_prix_millesime_pipeline(annee: int) -> Pipeline:
    """Fabrique un Pipeline pour un millésime DVF donné.

    Le catalog associé doit fournir `dvf_millesime_raw_<annee>` en entrée et
    `commune_prix_<annee>` / `dvf_points_<annee>` en sorties (voir
    tests/test_lot4_real_data.py). L'input catalog est paramétré par année ;
    on adapte son nom au paramètre fixe `dvf_millesime_raw` attendu par
    `prix_millesime()` via un wrapper.
    """

    def _node_func(con: duckdb.DuckDBPyConnection, **inputs: str) -> dict[str, str]:
        dvf_millesime_raw = inputs[f"dvf_millesime_raw_{annee}"]
        tables = prix_millesime(con, dvf_millesime_raw)
        return {
            f"commune_prix_{annee}": tables["commune_prix"],
            f"dvf_points_{annee}": tables["dvf_points"],
        }

    return Pipeline(
        nodes=[
            Node(
                func=_node_func,
                inputs=[f"dvf_millesime_raw_{annee}"],
                outputs=[f"commune_prix_{annee}", f"dvf_points_{annee}"],
                name=f"prix_millesime_{annee}",
            ),
        ]
    )
