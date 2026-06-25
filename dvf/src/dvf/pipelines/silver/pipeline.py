"""Pipeline silver : Bronze -> mutations dédoublonnées -> biens avec prix/m²."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import compute_price_per_m2, deduplicate_mutations, filter_quality


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=filter_quality,
                inputs=["bronze_geo_dvf", "params:natures_mutation_retenues"],
                outputs="dvf_clean",
                name="filter_quality",
            ),
            node(
                func=deduplicate_mutations,
                inputs=["dvf_clean", "params:types_local_batis"],
                outputs="silver_mutations",
                name="deduplicate_mutations",
            ),
            node(
                func=compute_price_per_m2,
                inputs=[
                    "silver_mutations",
                    "params:max_locaux_for_ppm2",
                    "params:ppm2_clip_quantiles",
                ],
                outputs="silver_biens_ppm2",
                name="compute_price_per_m2",
            ),
        ]
    )
