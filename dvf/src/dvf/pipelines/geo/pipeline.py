"""Pipeline geo : contours administratifs Etalab -> tables référentiel PostGIS."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import load_communes_geo, load_departements_geo


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=load_communes_geo,
                inputs=["params:contours_etalab", "postgis_credentials"],
                outputs="communes_geo_count",
                name="load_communes_geo",
            ),
            node(
                func=load_departements_geo,
                inputs=["params:contours_etalab", "postgis_credentials"],
                outputs="departements_geo_count",
                name="load_departements_geo",
            ),
        ]
    )
