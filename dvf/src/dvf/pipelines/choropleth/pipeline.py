"""Pipeline choropleth : jointure précalculée géométrie × agrégats -> PostGIS.

Autonome : matérialise les tables ``choropleth_*`` à partir des tables PostGIS
déjà remplies par `geo` et `gold`. À ordonnancer après ces deux pipelines.
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    materialize_choropleth_commune,
    materialize_choropleth_departement,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=materialize_choropleth_commune,
                inputs="postgis_credentials",
                outputs="choropleth_commune_count",
                name="materialize_choropleth_commune",
            ),
            node(
                func=materialize_choropleth_departement,
                inputs="postgis_credentials",
                outputs="choropleth_departement_count",
                name="materialize_choropleth_departement",
            ),
        ]
    )
