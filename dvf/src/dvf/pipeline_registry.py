"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    ``choropleth`` matérialise une jointure des tables PostGIS remplies par
    ``geo`` et ``gold``, sans le déclarer comme dépendance de données (elle reste
    autonome, lançable seule via ``--pipeline=choropleth``). Kedro ne pouvant pas
    en déduire l'ordre, on l'EXCLUT du run par défaut : ``__default__`` peuple les
    tables, puis ``choropleth`` est lancée ensuite (cf. Justfile / futur DAG
    Airflow). L'ordre est porté par l'orchestration, pas par le graphe.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    pipelines = find_pipelines(raise_errors=True)
    pipelines["__default__"] = sum(
        p for name, p in pipelines.items() if name != "choropleth"
    )
    return pipelines
