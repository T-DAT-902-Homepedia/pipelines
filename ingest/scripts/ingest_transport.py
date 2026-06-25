#!/usr/bin/env python3
"""Ingestion bronze des données « transport en commun » (data.gouv.fr -> S3).

Dépose dans la zone bronze les 2 ressources du dataset « Nombre de stations de
transports en commun pour 1000 habitants ». Elles sont utilisées séparément en
aval (aucune jointure entre elles) :
  - nb-stations-tc-com.csv : nb d'arrêts par type, agrégé par commune (stats de couverture)
  - tc-coord.csv           : position GPS de chaque arrêt (carte, icône par type)

Usage (depuis pipeline/) :
    uv run python scripts/ingest_transport.py

Variables d'environnement : HOMEPEDIA_S3_BUCKET (+ AWS_* / AWS_ENDPOINT_URL).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Rend le package `homepedia_ingest` importable quand on lance le script directement.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_ingest import ingest_datagouv_to_s3  # noqa: E402

DATASET = "transport"
RESOURCES = {
    "2a1b349d-4d96-4bd5-8b83-bf8e501fb96b": "nb-stations-tc-com (couverture par commune)",
    "37ecfc6a-ee47-44eb-bdb3-edbefcbb0eac": "tc-coord (coordonnées des arrêts)",
}


def main() -> int:
    for resource_id, label in RESOURCES.items():
        print(f"[bronze:transport] {label} — {resource_id}")
        res = ingest_datagouv_to_s3(resource_id, DATASET)
        status = "SKIP (déjà présent)" if res["status"] == "skipped" else "OK upload"
        print(f"  -> {status} : s3://{res['bucket']}/{res['key']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
