"""Schéma explicite du CSV Geo DVF (40 colonnes).

Pas d'``inferSchema`` (coûteux et instable sur 3,7M lignes). 

Les codes administratifs/postaux restent en String pour préserver les zéros à gauche
(INSEE, code postal, départements Corse 2A/2B et DOM 97x/98x).
"""

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


def _s(name: str) -> StructField:
    return StructField(name, StringType(), True)


def _d(name: str) -> StructField:
    return StructField(name, DoubleType(), True)


GEO_DVF_SCHEMA = StructType(
    [
        _s("id_mutation"),
        _s("date_mutation"),
        _s("numero_disposition"),
        _s("nature_mutation"),
        _d("valeur_fonciere"),
        _s("adresse_numero"),
        _s("adresse_suffixe"),
        _s("adresse_nom_voie"),
        _s("adresse_code_voie"),
        _s("code_postal"),
        _s("code_commune"),
        _s("nom_commune"),
        _s("code_departement"),
        _s("ancien_code_commune"),
        _s("ancien_nom_commune"),
        _s("id_parcelle"),
        _s("ancien_id_parcelle"),
        _s("numero_volume"),
        _s("lot1_numero"),
        _d("lot1_surface_carrez"),
        _s("lot2_numero"),
        _d("lot2_surface_carrez"),
        _s("lot3_numero"),
        _d("lot3_surface_carrez"),
        _s("lot4_numero"),
        _d("lot4_surface_carrez"),
        _s("lot5_numero"),
        _d("lot5_surface_carrez"),
        _s("nombre_lots"),
        _s("code_type_local"),
        _s("type_local"),
        _d("surface_reelle_bati"),
        _d("nombre_pieces_principales"),
        _s("code_nature_culture"),
        _s("nature_culture"),
        _s("code_nature_culture_speciale"),
        _s("nature_culture_speciale"),
        _d("surface_terrain"),
        _d("longitude"),
        _d("latitude"),
    ]
)
