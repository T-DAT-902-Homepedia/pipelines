"""Pipeline gold transport Homepedia : silver parquet -> PostGIS.

La couche silver (déjà nettoyée/typée par l'étape Spark) est lue depuis MinIO/S3,
puis chargée telle quelle dans les tables gold servies par l'API.
"""

from .pg import connect, dsn
from .silver import read_silver

__all__ = ["connect", "dsn", "read_silver"]
