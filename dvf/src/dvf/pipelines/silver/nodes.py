"""Nodes du pipeline silver : nettoyage, dédoublonnage, prix au m².

Règles méthodologiques :
- ``valeur_fonciere`` est constante par mutation -> JAMAIS ``sum``.
- Une mutation = N lignes (parcelle × local) -> dédoublonner par ``id_mutation``.
- Prix/m² uniquement sur mutations mono-bien bâti (attribution non ambiguë).
- Bornage des aberrations PAR département × type_local (un seuil national écraserait Paris).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def filter_quality(bronze: DataFrame, natures_retenues: list[str]) -> DataFrame:
    """Restreint aux natures de mutation retenues et aux valeurs exploitables."""
    return bronze.filter(
        F.col("nature_mutation").isin(natures_retenues)
        & F.col("valeur_fonciere").isNotNull()
        & (F.col("valeur_fonciere") > 0)
    )


def deduplicate_mutations(clean: DataFrame, types_batis: list[str]) -> DataFrame:
    """
    Réduit à une ligne par mutation, caractérisée.

    ``valeur_fonciere`` étant constante par mutation, on la récupère via ``first`` (jamais ``sum``).

    La surface bâtie totale n'agrège que les locaux bâtis (Maison/Appartement) ; 

    les dépendances/terrains en sont exclus.
    """
    est_bati = F.col("type_local").isin(types_batis)
    surface_si_bati = F.when(est_bati, F.col("surface_reelle_bati"))

    return clean.groupBy("id_mutation").agg(
        F.first("date_mutation", ignorenulls=True).alias("date_mutation"),
        F.first("nature_mutation", ignorenulls=True).alias("nature_mutation"),
        F.first("valeur_fonciere", ignorenulls=True).alias("valeur_fonciere"),
        F.first("code_commune", ignorenulls=True).alias("code_commune"),
        F.first("nom_commune", ignorenulls=True).alias("nom_commune"),
        F.first("code_departement", ignorenulls=True).alias("code_departement"),
        F.first("longitude", ignorenulls=True).alias("longitude"),
        F.first("latitude", ignorenulls=True).alias("latitude"),
        F.sum(F.when(est_bati, 1).otherwise(0)).alias("nb_locaux_batis"),
        F.sum(
            F.when(F.col("type_local") == "Maison", 1).otherwise(0)
        ).alias("nb_maisons"),
        F.sum(
            F.when(F.col("type_local") == "Appartement", 1).otherwise(0)
        ).alias("nb_appartements"),
        F.sum(surface_si_bati).alias("surface_bati_totale"),
        # type_local représentatif quand la mutation est mono-bien bâti.
        F.first(
            F.when(est_bati, F.col("type_local")), ignorenulls=True
        ).alias("type_local"),
    )


def compute_price_per_m2( mutations: DataFrame, max_locaux: int, clip_quantiles: dict[str, float]) -> DataFrame:
    """
    Prix au m² sur mutations mono-bien bâti, borné par département × type.

    On ne garde que les mutations contenant exactement ``max_locaux`` local(aux)
    bâti(s) (1 par défaut) avec une surface > 0 : l'attribution valeur->surface
    y est non ambiguë. 

    Le bornage p_low/p_high est calculé indépendamment pour chaque (département, type_local).
    """
    biens = (
        mutations.filter(
            (F.col("nb_locaux_batis") == max_locaux)
            & F.col("surface_bati_totale").isNotNull()
            & (F.col("surface_bati_totale") > 0)
        )
        .withColumn(
            "prix_m2", F.col("valeur_fonciere") / F.col("surface_bati_totale")
        )
    )

    grp = Window.partitionBy("code_departement", "type_local")
    borne = (
        biens.withColumn(
            "p_low",
            F.percentile_approx("prix_m2", clip_quantiles["lower"]).over(grp),
        )
        .withColumn(
            "p_high",
            F.percentile_approx("prix_m2", clip_quantiles["upper"]).over(grp),
        )
    )
    return (
        borne.filter(
            (F.col("prix_m2") >= F.col("p_low"))
            & (F.col("prix_m2") <= F.col("p_high"))
        )
        .drop("p_low", "p_high")
    )
