"""Vérifie le portage des sources communales simples (revenus, risques,
tourisme, securite, equipements, emploi, dpe) contre les vraies données
locales, en comparant aux comptes déjà matérialisés dans
exploration/data/exploration.duckdb (base de référence).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from duckpipe.catalog import Catalog
from duckpipe.connection import get_connection
from duckpipe.datasets.csv import CsvDataset
from duckpipe.datasets.geojson import GeoJsonDataset
from duckpipe.datasets.json_dataset import JsonDataset
from duckpipe.datasets.memory import MemoryDataset
from duckpipe.datasets.parquet import ParquetDataset
from duckpipe.pipelines.dpe import dpe_pipeline
from duckpipe.pipelines.emploi import emploi_pipeline
from duckpipe.pipelines.equipements import equipements_pipeline
from duckpipe.pipelines.geometries import geometries_pipeline
from duckpipe.pipelines.revenus import revenus_pipeline
from duckpipe.pipelines.risques import risques_pipeline
from duckpipe.pipelines.securite import securite_pipeline
from duckpipe.pipelines.tourisme import tourisme_pipeline

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

# Comptes de référence observés dans exploration/data/exploration.duckdb
REF_COUNTS = {
    "revenus": 31248,
    "risques": 34597,
    "tourisme": 34884,
    "securite": 34907,
    "equipements": 34871,
    "emploi": 34876,
    "dpe": 199158,
}


@pytest.fixture
def con_with_geom(tmp_path: Path):
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


def test_revenus(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("revenus_raw", CsvDataset(
        str(EXPLORATION_RAW / "revenus_commune_filosofi_2021.csv"),
        read_csv_kwargs={"delim": ";", "ignore_errors": True},
    ))
    catalog.add("revenus", ParquetDataset(str(tmp_path / "revenus.parquet")))
    revenus_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM revenus").fetchone()[0]
    assert n == REF_COUNTS["revenus"]


def test_risques(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("risques_raw", CsvDataset(
        str(EXPLORATION_RAW / "catnat_gaspar.csv"),
        read_csv_kwargs={"delim": ";", "ignore_errors": True},
    ))
    catalog.add("risques", ParquetDataset(str(tmp_path / "risques.parquet")))
    risques_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM risques").fetchone()[0]
    assert n == REF_COUNTS["risques"]


def test_tourisme(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("tourisme_raw", CsvDataset(
        str(EXPLORATION_RAW / "base-ic-logement-2021.CSV"),
        read_csv_kwargs={"delim": ";", "ignore_errors": True},
    ))
    catalog.add("tourisme", ParquetDataset(str(tmp_path / "tourisme.parquet")))
    tourisme_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM tourisme").fetchone()[0]
    assert n == REF_COUNTS["tourisme"]


def test_securite(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("securite_raw", CsvDataset(
        str(EXPLORATION_RAW / "securite_ssmsi_communale.csv.gz"),
        read_csv_kwargs={"delim": ";", "compression": "gzip", "ignore_errors": True},
    ))
    catalog.add("securite", ParquetDataset(str(tmp_path / "securite.parquet")))
    securite_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM securite").fetchone()[0]
    assert n == REF_COUNTS["securite"]


def test_equipements(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("equipements_raw", CsvDataset(
        str(EXPLORATION_RAW / "DS_BPE_2024_data.csv"),
        read_csv_kwargs={"delim": ";", "ignore_errors": True},
    ))
    catalog.add("equipements", ParquetDataset(str(tmp_path / "equipements.parquet")))
    equipements_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM equipements").fetchone()[0]
    assert n == REF_COUNTS["equipements"]


def test_emploi(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("emploi_raw", CsvDataset(
        str(EXPLORATION_RAW / "base_cc_emploi_pop_active_2021.CSV"),
        read_csv_kwargs={"delim": ";", "ignore_errors": True},
    ))
    catalog.add("emploi", ParquetDataset(str(tmp_path / "emploi.parquet")))
    emploi_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM emploi").fetchone()[0]
    assert n == REF_COUNTS["emploi"]


def test_dpe(con_with_geom, tmp_path: Path) -> None:
    con, catalog = con_with_geom
    catalog.add("dpe_raw", JsonDataset(str(EXPLORATION_RAW / "dpe_sample.jsonl")))
    catalog.add("dpe", ParquetDataset(str(tmp_path / "dpe.parquet")))
    dpe_pipeline.run(con, catalog)
    n = con.execute("SELECT count(*) FROM dpe").fetchone()[0]
    assert n == REF_COUNTS["dpe"]
