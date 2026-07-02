"""Vérifie le portage geometries/transport contre les vraies données locales
de exploration/data/raw/, en comparant les volumétries à celles déjà
matérialisées dans exploration/data/exploration.duckdb (base de référence).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from duckpipe.catalog import Catalog
from duckpipe.connection import get_connection
from duckpipe.datasets.csv import CsvDataset
from duckpipe.datasets.geojson import GeoJsonDataset
from duckpipe.datasets.memory import MemoryDataset
from duckpipe.pipelines.geometries import geometries_pipeline
from duckpipe.pipelines.transport import transport_pipeline

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

# Comptes de référence observés dans exploration/data/exploration.duckdb
REF_N_COMMUNES = 34928
REF_N_DEPARTEMENTS = 109


@pytest.fixture
def real_con():
    con = get_connection()
    yield con
    con.close()


def test_geometries_matches_reference_counts(real_con) -> None:
    catalog = (
        Catalog()
        .add("communes_raw", GeoJsonDataset(str(EXPLORATION_RAW / "communes-50m.geojson")))
        .add("depts_raw", GeoJsonDataset(str(EXPLORATION_RAW / "departements-50m.geojson")))
        .add("commune_geom", MemoryDataset())  # colonne GEOMETRY : pas de round-trip fichier ici
        .add("dept_geom", MemoryDataset())
    )

    geometries_pipeline.run(real_con, catalog)

    n_commune = real_con.execute("SELECT count(*) FROM commune_geom").fetchone()[0]
    n_dept = real_con.execute("SELECT count(*) FROM dept_geom").fetchone()[0]

    assert n_commune == REF_N_COMMUNES
    assert n_dept == REF_N_DEPARTEMENTS


def test_transport_matches_reference_counts(real_con) -> None:
    catalog = (
        Catalog()
        .add("communes_raw", GeoJsonDataset(str(EXPLORATION_RAW / "communes-50m.geojson")))
        .add("depts_raw", GeoJsonDataset(str(EXPLORATION_RAW / "departements-50m.geojson")))
        .add("commune_geom", MemoryDataset())
        .add("dept_geom", MemoryDataset())
        .add("arrets_raw", CsvDataset(str(EXPLORATION_RAW / "arrets_transport.csv")))
        .add("arrets", MemoryDataset())
        .add("commune_transport", MemoryDataset())
    )

    geometries_pipeline.run(real_con, catalog)
    transport_pipeline.run(real_con, catalog)

    n_commune_transport = real_con.execute("SELECT count(*) FROM commune_transport").fetchone()[0]
    assert n_commune_transport == REF_N_COMMUNES
