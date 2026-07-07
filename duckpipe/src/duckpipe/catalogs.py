"""Catalogs par environnement : la source unique de vérité des chemins.

Reprend le layout GCS d'ARCHITECTURE.md (bronze/silver/gold). Le même
`build_catalog` sert en local (tests, racines vers un dossier) et en prod
(racines `gs://homepedia-data/...`) — seules les racines changent, cf.
ADR-0005.

Les chemins bronze sont dérivés de `sources.SOURCES` (une seule définition) ;
les géométries silver sont en GeoParquet (round-trip GEOMETRY natif DuckDB).
"""

from __future__ import annotations

from dataclasses import dataclass

from duckpipe.catalog import Catalog
from duckpipe.datasets import (
    CsvDataset,
    GeoJsonDataset,
    JsonDataset,
    MemoryDataset,
    ParquetDataset,
)
from duckpipe.fetch_climat import CLIMAT_BRONZE_PATH
from duckpipe.fetch_dpe import DPE_BRONZE_PATH
from duckpipe.sources import DVF_URL_TEMPLATE, SOURCES  # noqa: F401 — DVF_URL réexporté

GCS_BUCKET = "gs://homepedia-data"
# Bucket public servant les artefacts web statiques à la webapp (cf. ADR-0013).
WEB_BUCKET = "gs://homepedia-web"
# Millésimes DVF annexes intégrés à l'évolution des prix des fiches communes
# et aux séries annuelles charts/prix_series.json (2024 = année du run ;
# geo-dvf ne conserve que les ~5 derniers millésimes).
WEB_MILLESIMES = [2021, 2022, 2023, 2025]


@dataclass(frozen=True)
class Environment:
    bronze_root: str
    silver_root: str
    gold_root: str


PROD = Environment(
    bronze_root=f"{GCS_BUCKET}/bronze",
    silver_root=f"{GCS_BUCKET}/silver",
    gold_root=f"{GCS_BUCKET}/gold",
)


def local_environment(root: str) -> Environment:
    root = root.rstrip("/")
    return Environment(
        bronze_root=f"{root}/bronze",
        silver_root=f"{root}/silver",
        gold_root=f"{root}/gold",
    )


def get_environment(name: str, *, local_root: str = "data") -> Environment:
    if name == "prod":
        return PROD
    if name == "local":
        return local_environment(local_root)
    raise ValueError(f"environnement inconnu : {name!r} (attendu : local | prod)")


# Chemins silver relatifs par nom logique (dossiers d'ARCHITECTURE.md).
# `{year}` n'apparaît que sur les tables millésimées (DVF).
SILVER_PATHS = {
    "commune_geom": "communes_geom/communes_geom.parquet",  # GeoParquet
    "dept_geom": "dept_geom/dept_geom.parquet",  # GeoParquet
    # Variantes pré-simplifiées Etalab pour les LOD web (ADR-0013)
    "commune_geom_1000m": "communes_geom/communes_geom_1000m.parquet",
    "dept_geom_100m": "dept_geom/dept_geom_100m.parquet",
    "dept_geom_1000m": "dept_geom/dept_geom_1000m.parquet",
    "region_geom_1000m": "region_geom/region_geom_1000m.parquet",
    "dvf": "dvf_clean/year={year}/dvf.parquet",
    "commune_agg": "commune_agg/year={year}/commune_agg.parquet",
    "commune_agg_type": "commune_agg_type/year={year}/commune_agg_type.parquet",
    "dept_agg": "dept_agg/year={year}/dept_agg.parquet",
    "commune_transport": "transport_commune/transport_commune.parquet",
    "revenus": "revenus_commune/revenus.parquet",
    "risques": "risques_commune/risques.parquet",
    "tourisme": "tourisme_commune/tourisme.parquet",
    "securite": "securite_commune/securite.parquet",
    "equipements": "equipements_commune/equipements.parquet",
    "emploi": "emploi_commune/emploi.parquet",
    "dpe": "dpe_commune/dpe.parquet",
    "climat": "climat_commune/climat.parquet",
    "proximite_metropole": "proximite_commune/proximite_metropole.parquet",
    # Avis ville-ideale : produits par l'étape NLP externe (package ville_ideale),
    # pas par un pipeline duckpipe. Chemins stables (non millésimés).
    "avis": "avis_clean/avis.parquet",
    "avis_segments": "avis_nlp/segments.parquet",
    "avis_tokens": "avis_nlp/tokens.parquet",
}

SEMICOLON_CSV = {"delim": ";", "ignore_errors": True}


def gold_score_path(env: Environment, run_date: str) -> str:
    return f"{env.gold_root}/score_territoire/run_date={run_date}/score.parquet"


def gold_latest_path(env: Environment) -> str:
    return f"{env.gold_root}/score_territoire/latest/score.parquet"


def gold_avis_path(env: Environment, run_date: str) -> str:
    return f"{env.gold_root}/avis_commune/run_date={run_date}/avis_commune.parquet"


def dq_report_path(env: Environment, kind: str, run_date: str) -> str:
    return f"{env.gold_root}/dq_reports/{kind}_{run_date}.json"


def web_root(env: Environment) -> str:
    """Racine des artefacts web : bucket public en prod, dossier local sinon."""
    if env.gold_root.startswith("gs://"):
        return f"{WEB_BUCKET}/v1"
    return f"{env.gold_root.rsplit('/', 1)[0]}/web/v1"


def web_run_root(env: Environment, run_date: str) -> str:
    return f"{web_root(env)}/runs/{run_date}"


def build_catalog(env: Environment, *, year: int, run_date: str) -> Catalog:
    """Catalog complet : chaque nom logique du registry vers son Dataset.

    Les entrées non utilisées par un pipeline donné sont inoffensives (un Node
    ne touche que ses inputs/outputs déclarés).
    """
    bronze = env.bronze_root.rstrip("/")
    catalog = Catalog()

    # --- Bronze : sources brutes -------------------------------------------
    dvf_bronze = f"{bronze}/dvf/year={year}/full.csv.gz"
    dvf_csv_options = {"compression": "gzip", "quote": '"', "escape": '"'}
    catalog.add("dvf_raw", CsvDataset(dvf_bronze, read_csv_kwargs=dvf_csv_options))
    catalog.add(
        f"dvf_millesime_raw_{year}", CsvDataset(dvf_bronze, read_csv_kwargs=dvf_csv_options)
    )
    catalog.add(
        "communes_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_communes'].bronze_path}"),
    )
    catalog.add(
        "depts_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_departements'].bronze_path}"),
    )
    catalog.add(
        "communes_1000m_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_communes_1000m'].bronze_path}"),
    )
    catalog.add(
        "depts_100m_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_departements_100m'].bronze_path}"),
    )
    catalog.add(
        "depts_1000m_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_departements_1000m'].bronze_path}"),
    )
    catalog.add(
        "regions_1000m_raw",
        GeoJsonDataset(f"{bronze}/{SOURCES['geometries_regions_1000m'].bronze_path}"),
    )
    catalog.add("arrets_raw", CsvDataset(f"{bronze}/{SOURCES['transport'].bronze_path}"))
    catalog.add(
        "revenus_raw",
        CsvDataset(f"{bronze}/{SOURCES['revenus'].bronze_path}", read_csv_kwargs=SEMICOLON_CSV),
    )
    catalog.add(
        "risques_raw",
        CsvDataset(
            f"{bronze}/{SOURCES['risques'].zip_member_bronze_path}",
            read_csv_kwargs=SEMICOLON_CSV,
        ),
    )
    catalog.add(
        "tourisme_raw",
        CsvDataset(
            f"{bronze}/{SOURCES['tourisme'].zip_member_bronze_path}",
            read_csv_kwargs=SEMICOLON_CSV,
        ),
    )
    catalog.add(
        "securite_raw",
        CsvDataset(
            f"{bronze}/{SOURCES['securite'].bronze_path}",
            read_csv_kwargs={**SEMICOLON_CSV, "compression": "gzip"},
        ),
    )
    catalog.add(
        "equipements_raw",
        CsvDataset(
            f"{bronze}/{SOURCES['equipements'].zip_member_bronze_path}",
            read_csv_kwargs=SEMICOLON_CSV,
        ),
    )
    catalog.add(
        "emploi_raw",
        CsvDataset(
            f"{bronze}/{SOURCES['emploi'].zip_member_bronze_path}",
            read_csv_kwargs=SEMICOLON_CSV,
        ),
    )
    catalog.add("dpe_raw", JsonDataset(f"{bronze}/{DPE_BRONZE_PATH}"))
    catalog.add("climat_stations_raw", CsvDataset(f"{bronze}/{CLIMAT_BRONZE_PATH}"))
    catalog.add(
        "communes_raw_metropoles",
        CsvDataset(f"{bronze}/{SOURCES['proximite_metropole'].bronze_path}"),
    )

    # --- Silver : tables nettoyées (Parquet/GeoParquet) ----------------------
    for name, relative_path in SILVER_PATHS.items():
        path = f"{env.silver_root}/{relative_path.format(year=year)}"
        catalog.add(name, ParquetDataset(path))
    catalog.add("arrets", MemoryDataset())  # intermédiaire transport, jamais persisté
    # Millésimes DVF annexes : l'année du run + ceux consommés par l'export web
    # (l'évolution des prix des fiches communes, cf. WEB_MILLESIMES).
    for annee in {year, *WEB_MILLESIMES}:
        catalog.add(
            f"commune_prix_{annee}",
            ParquetDataset(
                f"{env.silver_root}/commune_prix/year={annee}/commune_prix.parquet"
            ),
        )

    # --- Gold ----------------------------------------------------------------
    catalog.add("score_territoire", ParquetDataset(gold_score_path(env, run_date)))
    catalog.add("avis_commune", ParquetDataset(gold_avis_path(env, run_date)))

    return catalog
