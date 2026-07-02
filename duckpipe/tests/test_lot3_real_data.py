"""Vérifie le portage climat/proximite_metropole (jointures Haversine) contre
les vraies données locales, en comparant aux comptes déjà matérialisés dans
exploration/data/exploration.duckdb (base de référence).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from duckpipe.catalog import Catalog
from duckpipe.connection import get_connection
from duckpipe.datasets.csv import CsvDataset
from duckpipe.datasets.geojson import GeoJsonDataset
from duckpipe.datasets.memory import MemoryDataset
from duckpipe.datasets.parquet import ParquetDataset
from duckpipe.pipelines.climat import climat_pipeline
from duckpipe.pipelines.geometries import geometries_pipeline
from duckpipe.pipelines.proximite_metropole import proximite_metropole_pipeline

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

# Comptes de référence observés dans exploration/data/exploration.duckdb
REF_CLIMAT = 33747
REF_PROXIMITE_METROPOLE = 34922


@pytest.fixture
def con_with_geom():
    con = get_connection()
    catalog = (
        Catalog()
        .add("communes_raw", GeoJsonDataset(str(EXPLORATION_RAW / "communes-50m.geojson")))
        .add("depts_raw", GeoJsonDataset(str(EXPLORATION_RAW / "departements-50m.geojson")))
        .add("commune_geom", MemoryDataset())
        .add("dept_geom", MemoryDataset())
    )
    geometries_pipeline.run(con, catalog)
    yield con, catalog
    con.close()


def test_climat(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add(
        "climat_stations_raw",
        CsvDataset(str(EXPLORATION_RAW / "fiches_climatologiques_stations.csv")),
    )
    catalog.add("climat", ParquetDataset(str(tmp_path / "climat.parquet")))
    climat_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM climat").fetchone()[0]
    assert n == REF_CLIMAT


def test_proximite_metropole(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add(
        "communes_raw_metropoles",
        CsvDataset(str(EXPLORATION_RAW / "communes-france-2024.csv")),
    )
    catalog.add("proximite_metropole", ParquetDataset(str(tmp_path / "proximite.parquet")))
    proximite_metropole_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM proximite_metropole").fetchone()[0]
    assert n == REF_PROXIMITE_METROPOLE
