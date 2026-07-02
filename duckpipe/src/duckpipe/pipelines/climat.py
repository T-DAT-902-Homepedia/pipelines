from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Seuils de plausibilité repris de exploration/src/preprocess.py (normales
# Météo-France 1991-2020).
ENSOLEILLEMENT_MIN = 500
ENSOLEILLEMENT_MAX = 3200
DIST_STATION_MAX_KM = 150


def climat(con: duckdb.DuckDBPyConnection, climat_stations_raw: str, commune_geom: str) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_climat` +
    `preprocess.py::clean_climat`.

    Table `climat` : (code_commune, ensoleillement_h_an, jours_ensoleilles,
    temperature_moy_annuelle, dist_station_km), rattachée par station
    Météo-France la plus proche (distance Haversine sur le centroïde communal).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stations AS
        SELECT
            num_poste,
            TRY_CAST(lat AS DOUBLE) AS lat,
            TRY_CAST(lon AS DOUBLE) AS lon,
            TRY_CAST(ensoleillement_h_an AS DOUBLE) AS ensoleillement_h_an,
            TRY_CAST(temperature_moy_annuelle AS DOUBLE) AS temperature_moy_annuelle,
            CAST(ROUND(TRY_CAST(ensoleillement_h_an AS DOUBLE) / 6.5) AS INTEGER)
                AS jours_ensoleilles
        FROM {climat_stations_raw}
        WHERE num_poste IS NOT NULL
          AND TRY_CAST(lat AS DOUBLE) IS NOT NULL
          AND TRY_CAST(lon AS DOUBLE) IS NOT NULL
          AND TRY_CAST(ensoleillement_h_an AS DOUBLE) IS NOT NULL
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE climat AS
        WITH distances AS (
            SELECT
                g.code_commune,
                s.ensoleillement_h_an,
                s.jours_ensoleilles,
                s.temperature_moy_annuelle,
                acos(
                    sin(radians(ST_Y(ST_Centroid(g.geom)))) * sin(radians(s.lat)) +
                    cos(radians(ST_Y(ST_Centroid(g.geom)))) * cos(radians(s.lat)) *
                    cos(radians(s.lon - ST_X(ST_Centroid(g.geom))))
                ) * 6371 AS dist_station_km
            FROM {commune_geom} g
            CROSS JOIN stations s
        ),
        nearest AS (
            SELECT
                code_commune,
                arg_min(ensoleillement_h_an, dist_station_km) AS ensoleillement_h_an,
                arg_min(jours_ensoleilles, dist_station_km) AS jours_ensoleilles,
                arg_min(temperature_moy_annuelle, dist_station_km) AS temperature_moy_annuelle,
                min(dist_station_km) AS dist_station_km
            FROM distances
            GROUP BY code_commune
        )
        SELECT * FROM nearest
        WHERE ensoleillement_h_an BETWEEN {ENSOLEILLEMENT_MIN} AND {ENSOLEILLEMENT_MAX}
          AND dist_station_km <= {DIST_STATION_MAX_KM}
        """
    )
    return "climat"


climat_pipeline = Pipeline(
    nodes=[
        Node(
            func=climat,
            inputs=["climat_stations_raw", "commune_geom"],
            outputs=["climat"],
            name="climat",
        ),
    ]
)
