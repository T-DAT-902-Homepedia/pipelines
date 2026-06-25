"""CLI générique d'ingestion bronze vers S3.

Ingère n'importe quelle resource data.gouv.fr (CSV ou autre) dans la zone
bronze, sans écrire de script dédié :

    uv run python -m homepedia_ingest <resource_id> <dataset>

Variables d'environnement : HOMEPEDIA_S3_BUCKET (obligatoire), HOMEPEDIA_S3_PREFIX
(défaut "bronze"), AWS_ENDPOINT_URL (optionnel, MinIO/LocalStack) + credentials
AWS standard.
"""

from __future__ import annotations

import argparse

from .bronze import ingest_datagouv_to_s3


def main() -> int:
    parser = argparse.ArgumentParser(prog="homepedia_ingest", description=__doc__)
    parser.add_argument("resource_id", help="ID de la resource data.gouv.fr")
    parser.add_argument("dataset", help="Nom logique du dataset (clé de partition)")
    args = parser.parse_args()

    res = ingest_datagouv_to_s3(args.resource_id, args.dataset)
    if res["status"] == "skipped":
        print(f"[bronze] SKIP (déjà présent, même sha256) : s3://{res['bucket']}/{res['key']}")
    else:
        print(f"[bronze] OK upload : s3://{res['bucket']}/{res['key']}")
        print(f"[bronze] {res['size_bytes']} octets, sha256 {res['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
