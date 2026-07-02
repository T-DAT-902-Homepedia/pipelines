from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Bbox France (métropole + DOM), reprise de exploration/src/preprocess.py, large
# volontairement pour couvrir Guadeloupe/Martinique/Guyane/Réunion/Mayotte.
FR_LON_MIN, FR_LON_MAX = -62.0, 56.0
FR_LAT_MIN, FR_LAT_MAX = -22.0, 51.5


def clean_transport_stops(con: duckdb.DuckDBPyConnection, table: str = "arrets") -> None:
    """Écarte les arrêts hors bbox France (coordonnées aberrantes du fichier
    national GTFS agrégé). Adaptation de preprocess.py::clean_transport_stops.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM {table}
        WHERE lon BETWEEN {FR_LON_MIN} AND {FR_LON_MAX}
          AND lat BETWEEN {FR_LAT_MIN} AND {FR_LAT_MAX}
        """
    )


def transport(con: duckdb.DuckDBPyConnection, arrets_raw: str, commune_geom: str) -> dict[str, str]:
    """Adaptation de `exploration/src/ingest.py::ensure_transport`.

    Déduplique les arrêts sur (lat, lon) arrondis (le fichier national agrège
    plusieurs GTFS sans dédup), puis rattache chaque arrêt à sa commune par
    jointure spatiale point-in-polygon (ST_Contains) et calcule la densité
    d'arrêts/km².
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE arrets AS
        SELECT DISTINCT
            round(CAST(stop_lat AS DOUBLE), 5) AS lat,
            round(CAST(stop_lon AS DOUBLE), 5) AS lon,
            ST_Point(round(CAST(stop_lon AS DOUBLE), 5),
                     round(CAST(stop_lat AS DOUBLE), 5)) AS geom
        FROM {arrets_raw}
        WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
        """
    )
    clean_transport_stops(con, "arrets")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE commune_transport AS
        SELECT g.code_commune,
               g.surface_km2,
               count(a.geom) AS nb_arrets,
               count(a.geom) / NULLIF(g.surface_km2, 0) AS densite_arrets_km2
        FROM {commune_geom} g
        LEFT JOIN arrets a ON ST_Contains(g.geom, a.geom)
        GROUP BY g.code_commune, g.surface_km2
        """
    )
    return {"arrets": "arrets", "commune_transport": "commune_transport"}


transport_pipeline = Pipeline(
    nodes=[
        Node(
            func=transport,
            inputs=["arrets_raw", "commune_geom"],
            outputs=["arrets", "commune_transport"],
            name="transport",
        ),
    ]
)
