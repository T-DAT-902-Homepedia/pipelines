"""Vérifie le portage prix_millesime + quality.py contre les vraies données
locales, en comparant aux comptes de exploration/data/exploration.duckdb.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from duckpipe import quality
from duckpipe.catalog import Catalog
from duckpipe.connection import get_connection
from duckpipe.datasets.csv import CsvDataset
from duckpipe.datasets.geojson import GeoJsonDataset
from duckpipe.datasets.memory import MemoryDataset
from duckpipe.pipelines.geometries import geometries_pipeline
from duckpipe.pipelines.prix_millesime import make_prix_millesime_pipeline

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

# Comptes de référence observés dans exploration/data/exploration.duckdb
REF_PRIX_MILLESIME = {2021: 31447, 2022: 31248}


@pytest.fixture
def con():
    connection = get_connection()
    yield connection
    connection.close()


@pytest.mark.parametrize("annee", [2021, 2022])
def test_prix_millesime(con, annee: int) -> None:
    catalog = Catalog().add(
        f"dvf_millesime_raw_{annee}",
        CsvDataset(
            str(EXPLORATION_RAW / f"dvf_full_{annee}.csv.gz"),
            read_csv_kwargs={"compression": "gzip", "quote": '"', "escape": '"'},
        ),
    )
    catalog.add(f"commune_prix_{annee}", MemoryDataset())

    pipeline = make_prix_millesime_pipeline(annee)
    pipeline.run(con, catalog)

    n = con.execute(f"SELECT count(*) FROM commune_prix_{annee}").fetchone()[0]
    assert n == REF_PRIX_MILLESIME[annee]


def test_quality_validate_on_commune_geom(con) -> None:
    catalog = (
        Catalog()
        .add("communes_raw", GeoJsonDataset(str(EXPLORATION_RAW / "communes-50m.geojson")))
        .add("depts_raw", GeoJsonDataset(str(EXPLORATION_RAW / "departements-50m.geojson")))
        .add("commune_geom", MemoryDataset())
        .add("dept_geom", MemoryDataset())
    )
    geometries_pipeline.run(con, catalog)

    results = quality.validate(con, "commune_geom", quality.GEOM_RULES)
    assert all(r["statut"] == "OK" for r in results)


def test_quality_profile_and_coverage(con) -> None:
    catalog = (
        Catalog()
        .add("communes_raw", GeoJsonDataset(str(EXPLORATION_RAW / "communes-50m.geojson")))
        .add("depts_raw", GeoJsonDataset(str(EXPLORATION_RAW / "departements-50m.geojson")))
        .add("commune_geom", MemoryDataset())
        .add("dept_geom", MemoryDataset())
    )
    geometries_pipeline.run(con, catalog)

    prof = quality.profile(con, "commune_geom", numeric_cols=["surface_km2"])
    columns_profiled = {row["colonne"] for row in prof}
    assert "code_commune" in columns_profiled
    assert "surface_km2" in columns_profiled

    full_coverage_pct = 100.0
    cov = quality.coverage(con, [{"table": "commune_geom", "label": "geom"}])
    assert cov[0]["taux_%"] == full_coverage_pct
