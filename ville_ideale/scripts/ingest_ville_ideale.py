#!/usr/bin/env python3
"""Ingestion bronze des notes ville-ideale.fr (JSON PyvesB -> S3).

Source : https://github.com/PyvesB/elections-ideales/tree/villes_ratings_20260222
Données : notes moyennes par commune sur 9 critères (2893 communes avec notes).

Usage (depuis ville_ideale/) :
    uv run python scripts/ingest_ville_ideale.py

Variables d'environnement : HOMEPEDIA_S3_BUCKET (+ AWS_* / AWS_ENDPOINT_URL).
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "ingest"))

from homepedia_ingest.s3 import make_client, s3_bucket, s3_prefix, head_metadata, put_file  # noqa: E402

SOURCE_URL = (
    "https://raw.githubusercontent.com/PyvesB/elections-ideales/"
    "villes_ratings_20260222/villes_ratings.json"
)

DATASET = "ville_ideale"
OBJECT_KEY_TEMPLATE = "{prefix}/{dataset}/notes_par_commune.csv"

COLUMNS = [
    "slug",
    "code_commune",
    "nom_ville",
    "code_postal",
    "note_globale",
    "environnement",
    "transports",
    "securite",
    "sante",
    "sports_et_loisirs",
    "culture",
    "enseignement",
    "commerces",
    "qualite_de_vie",
]


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def to_csv(data: dict) -> tuple[bytes, str]:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS)
    writer.writeheader()

    for slug, v in data.items():
        # slug format : "metz_57463" -> code_commune = "57463"
        code = slug.rsplit("_", 1)[-1] if "_" in slug else ""
        writer.writerow({
            "slug": slug,
            "code_commune": code,
            "nom_ville": v.get("name", ""),
            "code_postal": v.get("postcode", ""),
            "note_globale": v.get("overall", ""),
            "environnement": v.get("environnement", ""),
            "transports": v.get("transports", ""),
            "securite": v.get("securite", ""),
            "sante": v.get("sante", ""),
            "sports_et_loisirs": v.get("sports_et_loisirs", ""),
            "culture": v.get("culture", ""),
            "enseignement": v.get("enseignement", ""),
            "commerces": v.get("commerces", ""),
            "qualite_de_vie": v.get("qualite_de_vie", ""),
        })

    content = buf.getvalue().encode("utf-8")
    sha256 = hashlib.sha256(content).hexdigest()
    return content, sha256


def main() -> int:
    print(f"[bronze:ville_ideale] Téléchargement depuis {SOURCE_URL}")
    data = fetch_json(SOURCE_URL)
    with_notes = sum(1 for v in data.values() if v.get("overall") is not None)
    print(f"  {len(data)} communes chargées, {with_notes} avec notes")

    content, sha256 = to_csv(data)

    client = make_client()
    bucket = s3_bucket()
    key = OBJECT_KEY_TEMPLATE.format(prefix=s3_prefix(), dataset=DATASET)

    existing = head_metadata(client, bucket, key)
    if existing and existing.get("sha256") == sha256:
        print(f"  -> SKIP (déjà présent, même sha256) : s3://{bucket}/{key}")
        return 0

    # Écriture temporaire pour upload
    tmp = Path("/tmp/notes_par_commune.csv")
    tmp.write_bytes(content)
    put_file(client, bucket, key, tmp, metadata={"sha256": sha256})
    print(f"  -> OK upload : s3://{bucket}/{key} ({len(content)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
