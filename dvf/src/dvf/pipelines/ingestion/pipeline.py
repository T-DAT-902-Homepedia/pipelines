"""Pipeline d'ingestion : geo_dvf_raw (CSV.gz local) -> Bronze (Parquet MinIO)."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import ensure_raw_csv, to_bronze


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ensure_raw_csv,
                inputs="params:geo_dvf_source_url",
                outputs="raw_csv_ready",
                name="ensure_raw_csv",
            ),
            node(
                func=to_bronze,
                inputs=[ "geo_dvf_raw", "raw_csv_ready", "params:bronze_repartitions" ],
                outputs="bronze_geo_dvf",
                name="to_bronze",
            ),
        ]
    )
