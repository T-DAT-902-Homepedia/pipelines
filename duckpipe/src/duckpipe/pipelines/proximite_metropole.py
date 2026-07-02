from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

METROPOLE_POP_MIN = 50_000
DIST_METROPOLE_MAX_KM = 500


def proximite_metropole(
    con: duckdb.DuckDBPyConnection, communes_raw_metropoles: str, commune_geom: str
) -> str:
    """Adaptation de `exploration/src/ingest_extra.py::ensure_proximite_metropole`
    + `preprocess.py::clean_proximite_metropole`.

    Table `proximite_metropole` : (code_commune, dist_metropole_km,
    nom_metropole) — distance Haversine à la métropole (>50k hab.) la plus
    proche.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE metropoles AS
        SELECT
            TRIM(code_insee) AS code_metropole,
            TRIM(nom_standard) AS nom_metropole,
            TRY_CAST(latitude_centre AS DOUBLE) AS lat,
            TRY_CAST(longitude_centre AS DOUBLE) AS lon,
            TRY_CAST(population AS BIGINT) AS population
        FROM {communes_raw_metropoles}
        WHERE TRY_CAST(population AS BIGINT) >= {METROPOLE_POP_MIN}
          AND TRY_CAST(latitude_centre AS DOUBLE) IS NOT NULL
          AND TRY_CAST(longitude_centre AS DOUBLE) IS NOT NULL
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE proximite_metropole AS
        WITH distances AS (
            SELECT
                g.code_commune,
                m.nom_metropole,
                acos(
                    sin(radians(ST_Y(ST_Centroid(g.geom)))) * sin(radians(m.lat)) +
                    cos(radians(ST_Y(ST_Centroid(g.geom)))) * cos(radians(m.lat)) *
                    cos(radians(m.lon - ST_X(ST_Centroid(g.geom))))
                ) * 6371 AS dist_metropole_km
            FROM {commune_geom} g
            CROSS JOIN metropoles m
        ),
        nearest AS (
            SELECT
                code_commune,
                arg_min(nom_metropole, dist_metropole_km) AS nom_metropole,
                min(dist_metropole_km) AS dist_metropole_km
            FROM distances
            GROUP BY code_commune
        )
        SELECT * FROM nearest
        WHERE dist_metropole_km <= {DIST_METROPOLE_MAX_KM}
        """
    )
    return "proximite_metropole"


proximite_metropole_pipeline = Pipeline(
    nodes=[
        Node(
            func=proximite_metropole,
            inputs=["communes_raw_metropoles", "commune_geom"],
            outputs=["proximite_metropole"],
            name="proximite_metropole",
        ),
    ]
)
