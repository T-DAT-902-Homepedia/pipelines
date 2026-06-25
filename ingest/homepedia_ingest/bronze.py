"""Ingestion de la couche bronze Homepedia vers S3.

Principe bronze : on dépose le fichier source **tel quel** dans S3, sans aucune
transformation ni typage (fidélité totale à la source). Le nettoyage et le
typage se feront en couche silver ; la mise en forme API en couche gold.

Clé S3 (style médaillon, partitionnée par date d'ingestion, compatible
Athena/BigQuery « Hive partitioning ») :

    {prefix}/source=datagouv/dataset=<dataset>/ingestion_date=YYYY-MM-DD/<resource_id>__<fichier>

Des métadonnées de lignage sont attachées à l'objet S3 (source-url, resource-id,
dataset-id, sha256, ingested-at), relisibles via head_object.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .datagouv import download, get_resource_meta
from .s3 import head_metadata, make_client, put_file, s3_bucket, s3_prefix


def _object_key(
    prefix: str, dataset: str, ingestion_date: str, resource_id: str, filename: str
) -> str:
    return (
        f"{prefix}/source=datagouv/dataset={dataset}"
        f"/ingestion_date={ingestion_date}/{resource_id}__{filename}"
    )


def ingest_datagouv_to_s3(resource_id: str, dataset: str) -> dict:
    """Télécharge une resource data.gouv.fr et la dépose brute dans S3 (bronze).

    Idempotent : si l'objet existe déjà à la même clé avec le même SHA-256,
    l'upload est sauté.

    Args:
        resource_id: identifiant de la resource data.gouv.fr.
        dataset: nom logique du jeu de données (clé de partition, ex. "transport").

    Returns:
        Dictionnaire récapitulatif : status (uploaded|skipped), bucket, key,
        sha256, size_bytes, source_url, ingested_at.
    """
    # Config S3 d'abord : fail-fast si HOMEPEDIA_S3_BUCKET manque (avant tout réseau).
    bucket = s3_bucket()
    prefix = s3_prefix()
    client = make_client()

    meta = get_resource_meta(resource_id)
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    ingested_at = now.isoformat()

    with tempfile.TemporaryDirectory() as tmp:
        dl = download(meta.url, Path(tmp))
        key = _object_key(prefix, dataset, ingestion_date, resource_id, dl.path.name)

        existing = head_metadata(client, bucket, key)
        if existing is not None and existing.get("sha256") == dl.sha256:
            return {
                "status": "skipped",
                "bucket": bucket,
                "key": key,
                "sha256": dl.sha256,
                "size_bytes": dl.size_bytes,
                "source_url": meta.url,
                "ingested_at": ingested_at,
            }

        # Métadonnées de lignage attachées à l'objet S3 (clés en tirets pour
        # rester portables AWS / MinIO ; valeurs forcément des chaînes).
        object_metadata = {
            "source": "datagouv",
            "dataset": dataset,
            "resource-id": resource_id,
            "dataset-id": meta.dataset_id,
            "source-url": meta.url,
            "sha256": dl.sha256,
            "ingested-at": ingested_at,
        }
        put_file(client, bucket, key, dl.path, metadata=object_metadata)

    return {
        "status": "uploaded",
        "bucket": bucket,
        "key": key,
        "sha256": dl.sha256,
        "size_bytes": dl.size_bytes,
        "source_url": meta.url,
        "ingested_at": ingested_at,
    }
