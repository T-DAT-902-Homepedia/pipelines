"""Lecture des datasets parquet de la couche silver depuis MinIO/S3.

Utilise le système de fichiers S3 intégré à pyarrow (aucune dépendance boto3) :
sur un endpoint custom (MinIO), `endpoint_override` + `scheme` suffisent. Les
fichiers de contrôle Spark (`_SUCCESS`, `_metadata`) sont ignorés nativement par
le lecteur de dataset parquet (préfixes `_`/`.`).

Variables d'environnement (mêmes conventions que le pipeline DVF) :
  MINIO_ENDPOINT        (défaut http://localhost:9000)
  MINIO_ROOT_USER       (défaut homepedia)
  MINIO_ROOT_PASSWORD   (obligatoire en pratique)
  HOMEPEDIA_S3_BUCKET   (défaut homepedia)
  HOMEPEDIA_SILVER_PREFIX (défaut silver)
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs


def _s3_filesystem() -> fs.S3FileSystem:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    scheme, sep, host = endpoint.partition("://")
    if not sep:  # endpoint fourni sans schéma
        host, scheme = scheme, "http"
    return fs.S3FileSystem(
        endpoint_override=host,
        access_key=os.environ.get("MINIO_ROOT_USER", "homepedia"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        scheme=scheme,
    )


def read_silver(dataset: str) -> pa.Table:
    """Charge tout le dataset parquet `silver/<dataset>` en une table Arrow."""
    bucket = os.environ.get("HOMEPEDIA_S3_BUCKET", "homepedia")
    prefix = os.environ.get("HOMEPEDIA_SILVER_PREFIX", "silver").strip("/")
    path = f"{bucket}/{prefix}/{dataset}"
    return pq.read_table(path, filesystem=_s3_filesystem())
