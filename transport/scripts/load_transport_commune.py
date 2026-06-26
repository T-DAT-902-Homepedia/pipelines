#!/usr/bin/env python3
"""Gold transport : silver/transport_stations (parquet) -> table transport_commune.

Le silver fournit une ligne par (commune, mode) pour les 5 modes réels (bus,
tramway, métro, train, autres) — mais PAS le mode synthétique 'ALL'. Ce script
charge les modes réels (TRUNCATE + COPY) puis calcule 'ALL' en SQL : somme des
stations de la commune, population inchangée, densité = stations/1000 hab. Le
mode 'ALL' est le défaut de l'API, il doit donc exister. Idempotent. La source
de vérité du schéma reste `schemas/schemas/08_transport_commune.sql`.

Usage (depuis pipelines/transport/) :
    set -a; source .env; set +a
    uv run python scripts/load_transport_commune.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Rend le package `homepedia_transport` importable en lancement direct.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_transport import connect, read_silver  # noqa: E402

COLUMNS = [
    "code_insee",
    "nom_commune",
    "route_type",
    "nb_stations",
    "population",
    "stations_per_1000hab",
    "annee",
]


def main() -> int:
    table = read_silver("transport_stations").select(COLUMNS)
    rows = table.to_pylist()
    print(f"[gold:transport_commune] {len(rows)} lignes lues du silver")

    cols = ", ".join(COLUMNS)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE transport_commune")
        with cur.copy(f"COPY transport_commune ({cols}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([r[c] for c in COLUMNS])

        # Mode synthétique 'ALL' = agrégat des modes réels par commune.
        # densité = total stations / population * 1000 (arrondi à 3 décimales,
        # comme le silver) ; NULL si population nulle.
        cur.execute(
            """
            INSERT INTO transport_commune
                (code_insee, nom_commune, route_type, nb_stations,
                 population, stations_per_1000hab, annee)
            SELECT code_insee, max(nom_commune), 'ALL',
                   sum(nb_stations),
                   max(population),
                   round(
                       (sum(nb_stations)::numeric * 1000)
                       / NULLIF(max(population), 0), 3
                   )::double precision,
                   max(annee)
            FROM transport_commune
            GROUP BY code_insee
            """
        )

        # Densité spatiale (stations/km²) : aire calculée une fois par commune
        # depuis commune_geometry (geom_low suffit, l'aire est insensible à la
        # simplification), appliquée à toutes les lignes (modes + 'ALL').
        cur.execute(
            """
            UPDATE transport_commune t
            SET stations_per_km2 = round(
                (t.nb_stations / NULLIF(a.km2, 0))::numeric, 2
            )
            FROM (
                SELECT code_commune, ST_Area(geom_low::geography) / 1e6 AS km2
                FROM commune_geometry
            ) a
            WHERE a.code_commune = t.code_insee
            """
        )
        cur.execute(
            "SELECT count(*), count(DISTINCT route_type) FROM transport_commune"
        )
        n, modes = cur.fetchone()
    print(f"  -> {n} lignes chargées, {modes} modes (dont 'ALL')")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
