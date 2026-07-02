"""Cohérence registry <-> catalog : chaque nom logique déclaré par un
pipeline doit exister dans le catalog construit par build_catalog."""

from __future__ import annotations

from duckpipe import catalogs
from duckpipe.pipeline_registry import register_pipelines
from duckpipe.pipelines.prix_millesime import make_prix_millesime_pipeline

YEAR = 2024


def test_every_pipeline_io_is_in_catalog(tmp_path) -> None:
    env = catalogs.local_environment(str(tmp_path))
    catalog = catalogs.build_catalog(env, year=YEAR, run_date="2026-07-02")

    pipelines = dict(register_pipelines())
    pipelines["prix_millesime"] = make_prix_millesime_pipeline(YEAR)

    missing: list[str] = []
    for pipeline_name, pipeline in pipelines.items():
        for node in pipeline.nodes:
            for logical_name in [*node.inputs, *node.outputs]:
                if logical_name not in catalog._datasets:
                    missing.append(f"{pipeline_name}: {logical_name}")

    assert not missing, f"noms logiques absents du catalog : {missing}"


def test_prod_environment_paths() -> None:
    assert catalogs.PROD.bronze_root == "gs://homepedia-data/bronze"
    assert (
        catalogs.gold_latest_path(catalogs.PROD)
        == "gs://homepedia-data/gold/score_territoire/latest/score.parquet"
    )
    assert catalogs.gold_score_path(catalogs.PROD, "2026-07-02") == (
        "gs://homepedia-data/gold/score_territoire/run_date=2026-07-02/score.parquet"
    )
