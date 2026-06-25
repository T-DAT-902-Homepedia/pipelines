"""Pipeline d'ingestion bronze Homepedia (data.gouv.fr -> S3)."""

from .bronze import ingest_datagouv_to_s3
from .datagouv import DownloadResult, ResourceMeta, download, get_resource_meta
from .s3 import make_client, s3_bucket, s3_prefix

__all__ = [
    "ingest_datagouv_to_s3",
    "get_resource_meta",
    "download",
    "ResourceMeta",
    "DownloadResult",
    "make_client",
    "s3_bucket",
    "s3_prefix",
]
