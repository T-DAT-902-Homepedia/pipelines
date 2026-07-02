#!/usr/bin/env python3
"""Gold transport : silver/transport_commune (duckpipe) -> table PostGIS transport_commune.

Charge tel quel l'agrégat par commune produit par duckpipe (une ligne par
commune : nombre d'arrêts + densité d'arrêts/km²). TRUNCATE + COPY, idempotent.
La source de vérité du schéma reste `schemas/schemas/08_transport_commune.sql`.

Usage (depuis pipelines/transport/) :
    # 1. produire le silver avec duckpipe (une fois) :
    #    cd ../duckpipe && python -m duckpipe run geometries --env local \
    #                   && python -m duckpipe run transport  --env local
    # 2. charger dans PostGIS :
    set -a; source .env; set +a
    uv run python scripts/load_transport_commune.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Rend le package `homepedia_transport` importable en lancement direct.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_transport import connect, read_silver  # noqa: E402

COLUMNS = ["code_commune", "nb_arrets", "densite_arrets_km2"]


def main() -> int:
    table = read_silver("transport_commune").select(COLUMNS)
    rows = table.to_pylist()
    print(f"[gold:transport_commune] {len(rows)} communes lues du silver")

    cols = ", ".join(COLUMNS)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE transport_commune")
        with cur.copy(f"COPY transport_commune ({cols}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([r[c] for c in COLUMNS])
        cur.execute("SELECT count(*) FROM transport_commune")
        (n,) = cur.fetchone()
    print(f"  -> {n} communes chargées")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
