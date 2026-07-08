from __future__ import annotations

from duckpipe.node import Pipeline
from duckpipe.pipelines.avis import avis_pipeline
from duckpipe.pipelines.climat import climat_pipeline
from duckpipe.pipelines.dpe import dpe_pipeline
from duckpipe.pipelines.dvf import dvf_pipeline
from duckpipe.pipelines.emploi import emploi_pipeline
from duckpipe.pipelines.equipements import equipements_pipeline
from duckpipe.pipelines.geometries import geometries_pipeline, geometries_web_pipeline
from duckpipe.pipelines.proximite_metropole import proximite_metropole_pipeline
from duckpipe.pipelines.revenus import revenus_pipeline
from duckpipe.pipelines.risques import risques_pipeline
from duckpipe.pipelines.score import score_pipeline
from duckpipe.pipelines.securite import securite_pipeline
from duckpipe.pipelines.tourisme import tourisme_pipeline
from duckpipe.pipelines.transport import transport_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Point d'entrée unique que les futures tâches Airflow (PythonOperator)
    importeront pour récupérer un pipeline nommé, ex. `register_pipelines()["dvf"]`.

    `prix_millesime` n'est pas inclus ici : il se construit par année via
    `duckpipe.pipelines.prix_millesime.make_prix_millesime_pipeline(annee)`,
    pas comme un pipeline statique unique.
    """
    return {
        "dvf": dvf_pipeline,
        "geometries": geometries_pipeline,
        "geometries_web": geometries_web_pipeline,
        "transport": transport_pipeline,
        "revenus": revenus_pipeline,
        "risques": risques_pipeline,
        "tourisme": tourisme_pipeline,
        "securite": securite_pipeline,
        "equipements": equipements_pipeline,
        "emploi": emploi_pipeline,
        "dpe": dpe_pipeline,
        "climat": climat_pipeline,
        "proximite_metropole": proximite_metropole_pipeline,
        "score": score_pipeline,
        "avis": avis_pipeline,
    }
