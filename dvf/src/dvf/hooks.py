"""Hooks projet.

- ``after_context_created`` : initialise la SparkSession à partir de
  ``conf/base/spark.yml`` (config publique) et des credentials MinIO de
  ``conf/local/credentials.yml`` (endpoint + clés S3A, non commités).
- ``after_catalog_created`` : expose les credentials PostGIS (``postgis_admin``)
  au catalog sous ``postgis_credentials`` pour le pipeline geo (node Python pur
  qui se connecte via psycopg, hors Spark).
"""

from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog, MemoryDataset
from pyspark import SparkConf
from pyspark.sql import SparkSession


class DvfHooks:
    @hook_impl
    def after_context_created(self, context: KedroContext) -> None:
        spark_conf = SparkConf().setAll(
            list(context.config_loader["spark"].items())
        )

        # Injection des accès MinIO/S3A depuis les credentials locaux : on garde
        # endpoint + clés hors du fichier spark.yml versionné.
        minio = context.config_loader["credentials"].get("minio", {})
        if endpoint := minio.get("endpoint"):
            spark_conf.set("spark.hadoop.fs.s3a.endpoint", endpoint)
        if access_key := minio.get("access_key"):
            spark_conf.set("spark.hadoop.fs.s3a.access.key", access_key)
        if secret_key := minio.get("secret_key"):
            spark_conf.set("spark.hadoop.fs.s3a.secret.key", secret_key)

        spark_session = (
            SparkSession.builder.appName(context.project_path.name)
            .config(conf=spark_conf)
            .getOrCreate()
        )
        spark_session.sparkContext.setLogLevel("WARN")

        # Mémorise les credentials PostGIS pour les exposer au catalog ensuite.
        self._postgis_credentials = context.config_loader["credentials"].get(
            "postgis_admin", {}
        )

    @hook_impl
    def after_catalog_created(self, catalog: DataCatalog) -> None:
        # Le pipeline geo (psycopg) consomme `postgis_credentials` en input.
        catalog["postgis_credentials"] = MemoryDataset(
            getattr(self, "_postgis_credentials", {})
        )
