"""
Nodes du pipeline gold : agrégats cartographiques + table de transactions.

Sorties chargées en PostGIS :
- ``agg_commune`` / ``agg_departement`` : statistiques de prix/m² par maille
  administrative × type de bien (carto).
- ``transactions`` : biens au niveau individuel pour les filtres dynamiques de
  l'API (type, prix, surface, géo).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def _stats_exprs() -> list[Column]:
    """Statistiques de dispersion du prix/m² (médiane via percentile_approx)."""
    return [
        F.count("*").alias("nb_transactions"),
        F.percentile_approx("prix_m2", 0.5).alias("prix_m2_median"),
        F.percentile_approx("prix_m2", 0.25).alias("prix_m2_p25"),
        F.percentile_approx("prix_m2", 0.75).alias("prix_m2_p75"),
        F.round(F.avg("prix_m2"), 2).alias("prix_m2_moyen"),
        F.min("prix_m2").alias("prix_m2_min"),
        F.max("prix_m2").alias("prix_m2_max"),
    ]


def aggregate_by_commune(biens: DataFrame, seuil_fiabilite: int) -> DataFrame:
    """
    Agrège par commune × type_local.

    Marque (sans supprimer) les communes à faible volume via ``fiable``.
    """
    return (
        biens.groupBy(
            "code_departement", "code_commune", "nom_commune", "type_local"
        )
        .agg(*_stats_exprs())
        .withColumn("fiable", F.col("nb_transactions") >= seuil_fiabilite)
    )


def aggregate_by_departement(biens: DataFrame, seuil_fiabilite: int) -> DataFrame:
    """Agrège par département × type_local."""
    return (
        biens.groupBy("code_departement", "type_local")
        .agg(*_stats_exprs())
        .withColumn("fiable", F.col("nb_transactions") >= seuil_fiabilite)
    )


def build_transactions(biens: DataFrame) -> DataFrame:
    """Table de transactions niveau bien pour les filtres dynamiques de l'API."""
    return biens.select(
        "id_mutation",
        "date_mutation",
        "type_local",
        "valeur_fonciere",
        F.col("surface_bati_totale").alias("surface_bati"),
        F.round("prix_m2", 2).alias("prix_m2"),
        "code_commune",
        "nom_commune",
        "code_departement",
        "longitude",
        "latitude",
    )
