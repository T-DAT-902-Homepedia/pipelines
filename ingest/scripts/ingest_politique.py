#!/usr/bin/env python3
"""Ingestion bronze des résultats des municipales 2026 (data.gouv.fr -> S3).

Dépose dans la zone bronze les résultats par commune des deux tours (Ministère
de l'intérieur). Le maire et l'orientation politique ne sont PAS dans les
fichiers : ils seront DÉRIVÉS en couche silver (maire = tête de la liste
gagnante de la commune ; orientation = nuance de cette liste).

Usage (depuis pipeline/) :
    uv run python scripts/ingest_politique.py

Variables d'environnement : HOMEPEDIA_S3_BUCKET (+ AWS_* / AWS_ENDPOINT_URL).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Rend le package `homepedia_ingest` importable quand on lance le script directement.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_ingest import ingest_datagouv_to_s3  # noqa: E402

DATASET = "politique"
RESOURCES = {
    "4feeef01-24f7-4d5a-914f-8aa806f31ec2": "municipales 2026 — résultats communes tour 1",
    "6ff67a28-01bf-459e-beca-dd7aa8132dc1": "municipales 2026 — résultats communes tour 2",
}


def main() -> int:
    for resource_id, label in RESOURCES.items():
        print(f"[bronze:politique] {label} — {resource_id}")
        res = ingest_datagouv_to_s3(resource_id, DATASET)
        status = "SKIP (déjà présent)" if res["status"] == "skipped" else "OK upload"
        print(f"  -> {status} : s3://{res['bucket']}/{res['key']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
