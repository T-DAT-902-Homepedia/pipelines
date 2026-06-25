"""Client S3 minimal pour la zone bronze Homepedia (basé sur boto3).

Configuration par variables d'environnement :
  HOMEPEDIA_S3_BUCKET   (obligatoire) nom du bucket S3 cible
  HOMEPEDIA_S3_PREFIX   (défaut "bronze") préfixe racine des objets bronze
  AWS_ENDPOINT_URL      (optionnel) endpoint S3 custom — permet de pointer un
                        MinIO / LocalStack en local sans compte AWS réel

Les credentials AWS suivent la chaîne standard boto3 (~/.aws/credentials,
variables AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, profil, rôle IAM, ...).
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def s3_bucket() -> str:
    """Nom du bucket S3 cible (variable HOMEPEDIA_S3_BUCKET, obligatoire)."""
    bucket = os.environ.get("HOMEPEDIA_S3_BUCKET")
    if not bucket:
        raise RuntimeError(
            "Variable d'environnement HOMEPEDIA_S3_BUCKET manquante "
            "(nom du bucket S3 cible pour la zone bronze)."
        )
    return bucket


def s3_prefix() -> str:
    """Préfixe racine des objets bronze (variable HOMEPEDIA_S3_PREFIX, défaut 'bronze')."""
    return os.environ.get("HOMEPEDIA_S3_PREFIX", "bronze").strip("/")


def make_client():
    """Client S3 boto3. Honore AWS_ENDPOINT_URL si défini (MinIO/LocalStack).

    Sur un endpoint custom, on force le path-style addressing : sinon boto3 tente
    le virtual-host style (`http://<bucket>.<endpoint>`), qui ne résout pas en
    local (MinIO/LocalStack). Sur AWS réel (endpoint absent), on laisse le défaut.
    """
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or None
    config = Config(s3={"addressing_style": "path"}) if endpoint else None
    return boto3.client("s3", endpoint_url=endpoint, config=config)


def head_metadata(client, bucket: str, key: str) -> dict | None:
    """Métadonnées custom de l'objet s'il existe, sinon None.

    Sert au contrôle d'idempotence (comparaison du sha256 avant ré-upload).
    """
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in _NOT_FOUND_CODES:
            return None
        raise
    return resp.get("Metadata", {})


def put_file(
    client,
    bucket: str,
    key: str,
    path: Path,
    *,
    metadata: dict,
    content_type: str = "text/csv",
) -> None:
    """Dépose un fichier local dans S3 (upload multipart automatique si gros)."""
    client.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={"Metadata": metadata, "ContentType": content_type},
    )
