"""Profilage exploratoire du dataset Geo DVF 2025 en PySpark.

Objectif : produire les métriques de qualité et de volume nécessaires pour
1. comprendre la donnée (complétude, dédoublonnage des mutations, aberrations),
2. décider du moteur de stockage de l'API (volume des agrégats de sortie).

Ce script est la version exécutable du notebook notebooks/profiling_geo_dvf.ipynb.
Il décompresse le .gz en Parquet (gzip non-splittable -> 1 seul coeur sinon) puis
enchaîne les sections de profilage A->E.

Usage (depuis pipelines/dvf/, avec Java 17 actif via `sdk env`) :
    .venv/bin/python scripts/profiling_geo_dvf.py
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
RAW_CSV_GZ = DATA_DIR / "01_raw" / "geo_dvf.csv.gz"
INTERMEDIATE_PARQUET = DATA_DIR / "02_intermediate" / "geo_dvf.parquet"
REPORT_DIR = DATA_DIR / "08_reporting"

# Biens bâtis pour lesquels un prix au m² a du sens.
TYPES_BATIS = ["Maison", "Appartement"]

# Schéma explicite : pas d'inferSchema (coûteux + instable sur 3,7M lignes).
# Les codes restent en String pour préserver les zéros à gauche (INSEE, postal).
SCHEMA = StructType(
    [
        StructField("id_mutation", StringType(), True),
        StructField("date_mutation", StringType(), True),
        StructField("numero_disposition", StringType(), True),
        StructField("nature_mutation", StringType(), True),
        StructField("valeur_fonciere", DoubleType(), True),
        StructField("adresse_numero", StringType(), True),
        StructField("adresse_suffixe", StringType(), True),
        StructField("adresse_nom_voie", StringType(), True),
        StructField("adresse_code_voie", StringType(), True),
        StructField("code_postal", StringType(), True),
        StructField("code_commune", StringType(), True),
        StructField("nom_commune", StringType(), True),
        StructField("code_departement", StringType(), True),
        StructField("ancien_code_commune", StringType(), True),
        StructField("ancien_nom_commune", StringType(), True),
        StructField("id_parcelle", StringType(), True),
        StructField("ancien_id_parcelle", StringType(), True),
        StructField("numero_volume", StringType(), True),
        StructField("lot1_numero", StringType(), True),
        StructField("lot1_surface_carrez", DoubleType(), True),
        StructField("lot2_numero", StringType(), True),
        StructField("lot2_surface_carrez", DoubleType(), True),
        StructField("lot3_numero", StringType(), True),
        StructField("lot3_surface_carrez", DoubleType(), True),
        StructField("lot4_numero", StringType(), True),
        StructField("lot4_surface_carrez", DoubleType(), True),
        StructField("lot5_numero", StringType(), True),
        StructField("lot5_surface_carrez", DoubleType(), True),
        StructField("nombre_lots", StringType(), True),
        StructField("code_type_local", StringType(), True),
        StructField("type_local", StringType(), True),
        StructField("surface_reelle_bati", DoubleType(), True),
        StructField("nombre_pieces_principales", DoubleType(), True),
        StructField("code_nature_culture", StringType(), True),
        StructField("nature_culture", StringType(), True),
        StructField("code_nature_culture_speciale", StringType(), True),
        StructField("nature_culture_speciale", StringType(), True),
        StructField("surface_terrain", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
    ]
)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("profiling_geo_dvf")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def ingest_to_parquet(spark: SparkSession) -> DataFrame:
    """gz (non-splittable) -> Parquet partitionné. Débloque le parallélisme."""
    if INTERMEDIATE_PARQUET.exists():
        return spark.read.parquet(str(INTERMEDIATE_PARQUET))

    raw = spark.read.csv(
        str(RAW_CSV_GZ),
        schema=SCHEMA,
        header=True,
        sep=",",
        encoding="UTF-8",
    )
    # Repartition pour écrire plusieurs fichiers Parquet exploitables en parallèle.
    raw.repartition(32).write.mode("overwrite").parquet(str(INTERMEDIATE_PARQUET))
    return spark.read.parquet(str(INTERMEDIATE_PARQUET))


def section_a_completeness(df: DataFrame, report: dict) -> None:
    """Schéma & complétude : taux de null/vide par colonne, cardinalités."""
    total = df.count()
    report["total_rows"] = total

    # Taux de complétude (null OU chaîne vide pour les colonnes texte).
    exprs = []
    for field in df.schema.fields:
        c = F.col(field.name)
        if isinstance(field.dataType, StringType):
            missing = F.sum(F.when(c.isNull() | (F.trim(c) == ""), 1).otherwise(0))
        else:
            missing = F.sum(F.when(c.isNull(), 1).otherwise(0))
        exprs.append(missing.alias(field.name))
    missing_row = df.select(exprs).collect()[0].asDict()
    report["missing_rate"] = {
        k: round(v / total, 4) for k, v in missing_row.items()
    }

    report["cardinalities"] = {
        col: df.select(F.countDistinct(col)).collect()[0][0]
        for col in [
            "id_mutation",
            "code_commune",
            "code_departement",
            "nature_mutation",
            "type_local",
        ]
    }


def section_b_dedup(df: DataFrame, report: dict) -> None:
    """Dédoublonnage : distribution lignes/mutation, invariant valeur_fonciere."""
    per_mut = df.groupBy("id_mutation").agg(
        F.count("*").alias("n_lignes"),
        F.countDistinct("valeur_fonciere").alias("n_valeurs"),
        F.countDistinct("numero_disposition").alias("n_dispositions"),
    )
    report["lignes_par_mutation"] = per_mut.select(
        F.avg("n_lignes").alias("moyenne"),
        F.expr("percentile_approx(n_lignes, 0.5)").alias("mediane"),
        F.max("n_lignes").alias("max"),
    ).collect()[0].asDict()

    # Combien de mutations ont plusieurs valeurs_foncieres distinctes ?
    report["mutations_valeur_non_constante"] = per_mut.filter(
        F.col("n_valeurs") > 1
    ).count()
    report["mutations_multi_disposition"] = per_mut.filter(
        F.col("n_dispositions") > 1
    ).count()


def section_c_outliers(df: DataFrame, report: dict) -> None:
    """Aberrations sur valeur_fonciere et surface_reelle_bati."""
    vf = df.select("valeur_fonciere")
    report["valeur_fonciere"] = {
        "nulls": vf.filter(F.col("valeur_fonciere").isNull()).count(),
        "zero_ou_neg": vf.filter(F.col("valeur_fonciere") <= 0).count(),
        "quantiles_p1_p50_p99": df.approxQuantile(
            "valeur_fonciere", [0.01, 0.5, 0.99], 0.01
        ),
    }
    sb = df.filter(F.col("type_local").isin(TYPES_BATIS))
    report["surface_bati"] = {
        "nulls_ou_zero": sb.filter(
            F.col("surface_reelle_bati").isNull()
            | (F.col("surface_reelle_bati") <= 0)
        ).count(),
        "quantiles_p1_p50_p99": sb.approxQuantile(
            "surface_reelle_bati", [0.01, 0.5, 0.99], 0.01
        ),
    }


def section_d_price_per_m2(df: DataFrame, report: dict) -> None:
    """Prix au m² sur mutations mono-bien bâti (attribution non ambiguë)."""
    bati = df.filter(F.col("type_local").isin(TYPES_BATIS))
    # Mutations contenant exactement un local bâti principal.
    locaux_par_mut = bati.groupBy("id_mutation").agg(
        F.count("*").alias("n_batis")
    )
    mono = locaux_par_mut.filter(F.col("n_batis") == 1).select("id_mutation")

    biens = (
        bati.join(mono, "id_mutation")
        .filter(F.col("surface_reelle_bati") > 0)
        .filter(F.col("valeur_fonciere") > 0)
        .withColumn(
            "prix_m2", F.col("valeur_fonciere") / F.col("surface_reelle_bati")
        )
    )
    report["prix_m2"] = {
        "n_biens_mono": biens.count(),
        "par_type": {
            r["type_local"]: {
                "mediane": r["mediane"],
                "p25": r["p25"],
                "p75": r["p75"],
            }
            for r in biens.groupBy("type_local")
            .agg(
                F.expr("percentile_approx(prix_m2, 0.5)").alias("mediane"),
                F.expr("percentile_approx(prix_m2, 0.25)").alias("p25"),
                F.expr("percentile_approx(prix_m2, 0.75)").alias("p75"),
            )
            .collect()
        },
    }


def section_e_geography(df: DataFrame, report: dict) -> None:
    """Cohérence géographique + volume agrégats (décision stockage)."""
    total = report["total_rows"]
    report["sans_coordonnees"] = df.filter(
        F.col("longitude").isNull() | F.col("latitude").isNull()
    ).count()

    communes_actives = df.select("code_commune").distinct().count()
    report["communes_actives"] = communes_actives

    # Volume estimé de la table agrégée commune x type_bien (décision stockage).
    report["volume_agg_commune_x_type"] = (
        df.filter(F.col("type_local").isin(TYPES_BATIS))
        .select("code_commune", "type_local")
        .distinct()
        .count()
    )

    # Communes à faible volume (fiabilité statistique de la médiane locale).
    par_commune = df.groupBy("code_commune").count()
    report["communes_moins_5_tx"] = par_commune.filter(
        F.col("count") < 5
    ).count()


def main() -> None:
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    report: dict = {}

    df = ingest_to_parquet(spark).cache()

    section_a_completeness(df, report)
    section_b_dedup(df, report)
    section_c_outliers(df, report)
    section_d_price_per_m2(df, report)
    section_e_geography(df, report)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "profiling_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nRapport écrit dans {out}")

    spark.stop()


if __name__ == "__main__":
    main()
