"""Pipeline gold : biens prix/m² -> agrégats + transactions (chargés en PostGIS)."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    aggregate_by_commune,
    aggregate_by_departement,
    build_transactions,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=aggregate_by_commune,
                inputs=["silver_biens_ppm2", "params:seuil_fiabilite_commune"],
                outputs="gold_agg_commune",
                name="aggregate_by_commune",
            ),
            node(
                func=aggregate_by_departement,
                inputs=["silver_biens_ppm2", "params:seuil_fiabilite_commune"],
                outputs="gold_agg_departement",
                name="aggregate_by_departement",
            ),
            node(
                func=build_transactions,
                inputs="silver_biens_ppm2",
                outputs="gold_transactions",
                name="build_transactions",
            ),
        ]
    )
