#!/usr/bin/env python3
"""Gold transport : silver/transport_stops (parquet) -> table transport_stops.

Le silver fournit les arrêts propres avec longitude/latitude. Ce script ajoute en
PostGIS les deux colonnes dérivées nécessaires à l'API :
  - geom         : point 4326 construit depuis (longitude, latitude) ;
  - code_commune : commune contenante, par jointure spatiale ST_Contains sur
                   commune_geometry.geom_high (index gist). NULL hors territoire.

Chargement via table temporaire puis INSERT...SELECT : TRUNCATE + recharge,
idempotent. Schéma source de vérité : `schemas/schemas/09_transport_stops.sql`.

Usage (depuis pipelines/transport/) :
    set -a; source .env; set +a
    uv run python scripts/load_transport_stops.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_transport import connect, read_silver  # noqa: E402

COLUMNS = ["station_name", "route_type", "nb_lignes", "annee", "longitude", "latitude"]


def main() -> int:
    table = read_silver("transport_stops").select(COLUMNS)
    rows = table.to_pylist()
    print(f"[gold:transport_stops] {len(rows)} arrêts lus du silver")

    cols = ", ".join(COLUMNS)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE transport_stops")
        cur.execute(
            "CREATE TEMP TABLE _stops_in ("
            "station_name text, route_type text, nb_lignes integer, annee integer, "
            "longitude double precision, latitude double precision) ON COMMIT DROP"
        )
        with cur.copy(f"COPY _stops_in ({cols}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([r[c] for c in COLUMNS])

        # geom + reverse-geocoding spatial. ST_Contains exploite l'index gist sur
        # geom_high via l'opérateur && ; LATERAL ... LIMIT 1 évite les doublons
        # sur communes limitrophes.
        cur.execute(
            """
            INSERT INTO transport_stops
                (station_name, route_type, nb_lignes, annee,
                 longitude, latitude, geom, code_commune)
            SELECT t.station_name, t.route_type, t.nb_lignes, t.annee,
                   t.longitude, t.latitude,
                   ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326),
                   c.code_commune
            FROM _stops_in t
            LEFT JOIN LATERAL (
                SELECT g.code_commune
                FROM commune_geometry g
                WHERE ST_Contains(
                    g.geom_high,
                    ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)
                )
                LIMIT 1
            ) c ON true
            """
        )
        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE code_commune IS NULL) "
            "FROM transport_stops"
        )
        n, orphans = cur.fetchone()
    print(f"  -> {n} arrêts chargés ({orphans} hors commune, code_commune NULL)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
