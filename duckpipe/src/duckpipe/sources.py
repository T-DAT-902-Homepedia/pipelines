"""Registre déclaratif des sources opendata et de leur destination bronze.

URLs reprises de `exploration/src/ingest.py` et `ingest_extra.py`. Chaque
source décrit : l'URL (éventuellement avec repli data.gouv, les chemins
opendata étant instables), les en-têtes HTTP éventuels (INSEE filtre certains
User-Agent), le membre à extraire si l'archive est un zip, et le chemin
bronze relatif (layout d'ARCHITECTURE.md, à préfixer par la racine bronze —
locale en test, `gs://homepedia-data/bronze/` en prod).

Deux sources ne rentrent pas dans ce moule et ont leur module dédié :
`climat` (parsing de ~600 fiches Météo-France, cf. fetch_climat.py) et
`dpe` (API paginée ADEME, cf. fetch_dpe.py).
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

from duckpipe import fetch

BROWSER_UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}

DVF_URL_TEMPLATE = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/full.csv.gz"


@dataclass(frozen=True)
class SourceSpec:
    """Décrit comment obtenir un fichier brut et où le poser en bronze."""

    name: str
    url: str
    bronze_path: str  # relatif à la racine bronze
    headers: dict[str, str] | None = None
    zip_member: str | None = None  # si l'URL est un zip : membre à extraire
    zip_member_bronze_path: str | None = None  # destination bronze du membre extrait
    datagouv_dataset_id: str | None = None  # repli si l'URL figée est morte
    datagouv_fmt: str | None = None
    datagouv_title_contains: str | None = None


SOURCES: dict[str, SourceSpec] = {
    "geometries_communes": SourceSpec(
        name="geometries_communes",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/communes-50m.geojson"
        ),
        bronze_path="geom/communes-50m.geojson",
    ),
    "geometries_departements": SourceSpec(
        name="geometries_departements",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/departements-50m.geojson"
        ),
        bronze_path="geom/departements-50m.geojson",
    ),
    # Contours pré-simplifiés Etalab pour les LOD des choroplèthes web
    # (ADR-0013) : générés par le producteur avec une topologie cohérente,
    # là où ST_CoverageSimplify s'est révélé non déterministe (cf. ADR-0008).
    "geometries_communes_1000m": SourceSpec(
        name="geometries_communes_1000m",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/communes-1000m.geojson"
        ),
        bronze_path="geom/communes-1000m.geojson",
    ),
    "geometries_departements_100m": SourceSpec(
        name="geometries_departements_100m",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/departements-100m.geojson"
        ),
        bronze_path="geom/departements-100m.geojson",
    ),
    "geometries_departements_1000m": SourceSpec(
        name="geometries_departements_1000m",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/departements-1000m.geojson"
        ),
        bronze_path="geom/departements-1000m.geojson",
    ),
    "geometries_regions_1000m": SourceSpec(
        name="geometries_regions_1000m",
        url=(
            "https://etalab-datasets.geo.data.gouv.fr/contours-administratifs/2025/"
            "geojson/regions-1000m.geojson"
        ),
        bronze_path="geom/regions-1000m.geojson",
    ),
    # CONTOURS-IRIS® (coédition IGN/INSEE, Licence Ouverte 2.0) : maille
    # infra-communale des agrégats quartier. Édition FlatGeoBuf France entière
    # (DOM inclus), déjà généralisée moyenne échelle — aucune simplification
    # calculée en aval, même politique que les contours Etalab (ADR-0013).
    # Paris/Lyon/Marseille y sont codés par ARRONDISSEMENT (751xx/6938x/132xx),
    # comme dans le DVF. Repli si cette édition disparaît de la Géoplateforme :
    # GPKG (archive .7z, extracteur à écrire) ou GeoParquet IGN (encodage
    # GeoArrow non lu par duckdb-spatial à ce jour). BROWSER_UA requis : la
    # Géoplateforme refuse (403) le User-Agent urllib par défaut, comme l'INSEE.
    "geometries_iris": SourceSpec(
        name="geometries_iris",
        url=(
            "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/"
            "CONTOURS-IRIS_3-0__FLATGEOBUF_WGS84G_FRA_2026-01-01/contours_iris.fgb"
        ),
        bronze_path="geom/contours_iris.fgb",
        headers=BROWSER_UA,
    ),
    "transport": SourceSpec(
        name="transport",
        url="https://transport.data.gouv.fr/resources/81333/download",
        bronze_path="transport/arrets_transport.csv",
    ),
    "revenus": SourceSpec(
        name="revenus",
        url=(
            "https://static.data.gouv.fr/resources/revenu-des-francais-a-la-commune/"
            "20251210-134014/revenu-des-francais-a-la-commune-1765372688826.csv"
        ),
        bronze_path="revenus/revenus_commune_filosofi_2021.csv",
        datagouv_dataset_id="693975a12bf6e062df23b30e",
        datagouv_fmt="csv",
    ),
    "risques": SourceSpec(
        name="risques",
        url="https://files.georisques.fr/GASPAR/gaspar.zip",
        bronze_path="risques/gaspar.zip",
        # Géorisques horodate désormais les membres (catnat_gaspar_2026-06-29.csv)
        zip_member="catnat_gaspar*.csv",
        zip_member_bronze_path="risques/catnat_gaspar.csv",
    ),
    "tourisme": SourceSpec(
        name="tourisme",
        url=(
            "https://www.insee.fr/fr/statistiques/fichier/8268838/"
            "base-ic-logement-2021_csv.zip"
        ),
        bronze_path="tourisme/base-ic-logement-2021.zip",
        headers=BROWSER_UA,
        zip_member="base-ic-logement-2021.CSV",
        zip_member_bronze_path="tourisme/base-ic-logement-2021.CSV",
    ),
    "securite": SourceSpec(
        name="securite",
        url=(
            "https://static.data.gouv.fr/resources/bases-statistiques-communale-"
            "departementale-et-regionale-de-la-delinquance-enregistree-par-la-police-"
            "et-la-gendarmerie-nationales/20260326-124144/"
            "donnee-data.gouv-2025-geographie2025-produit-le2026-02-03.csv.gz"
        ),
        bronze_path="securite/securite_ssmsi_communale.csv.gz",
        datagouv_dataset_id="621df2954fa5a3b5a023e23c",
        datagouv_fmt="csv.gz",
        datagouv_title_contains="communale",
    ),
    "equipements": SourceSpec(
        name="equipements",
        url="https://www.insee.fr/fr/statistiques/fichier/8217527/DS_BPE_CSV_FR.zip",
        bronze_path="bpe/bpe_2024.zip",
        headers=BROWSER_UA,
        zip_member="DS_BPE_2024_data.csv",
        zip_member_bronze_path="bpe/DS_BPE_2024_data.csv",
    ),
    "emploi": SourceSpec(
        name="emploi",
        url=(
            "https://www.insee.fr/fr/statistiques/fichier/8202916/"
            "base-cc-emploi-pop-active-2021_csv.zip"
        ),
        bronze_path="emploi/base_cc_emploi_2021.zip",
        headers=BROWSER_UA,
        zip_member="base-cc-emploi-pop-active-2021.CSV",
        zip_member_bronze_path="emploi/base_cc_emploi_pop_active_2021.CSV",
    ),
    "proximite_metropole": SourceSpec(
        name="proximite_metropole",
        url=(
            "https://static.data.gouv.fr/resources/communes-et-villes-de-france-en-csv-"
            "excel-json-parquet-et-feather/20241126-160035/communes-france-2024.csv"
        ),
        bronze_path="proximite/communes-france-2024.csv",
        datagouv_dataset_id="6745d9ae4524d845d2138193",
        datagouv_fmt="csv",
        datagouv_title_contains="2024",
    ),
}


def dvf_source(year: int) -> SourceSpec:
    """Source DVF pour un millésime donné (URL stable par année chez geo-dvf)."""
    return SourceSpec(
        name=f"dvf_{year}",
        url=DVF_URL_TEMPLATE.format(year=year),
        bronze_path=f"dvf/year={year}/full.csv.gz",
    )


def _download_with_fallback(spec: SourceSpec, dest: str, *, force: bool) -> str:
    """Télécharge l'URL figée, avec repli data.gouv si elle est morte
    (même politique que exploration/src : les chemins opendata bougent)."""
    try:
        return fetch.download(spec.url, dest, headers=spec.headers, force=force)
    except Exception:
        if not spec.datagouv_dataset_id:
            raise
        dyn = fetch.datagouv_resource_url(
            spec.datagouv_dataset_id,
            fmt=spec.datagouv_fmt,
            title_contains=spec.datagouv_title_contains,
        )
        if not dyn:
            raise
        return fetch.download(dyn, dest, headers=spec.headers, force=force)


def ingest_source(spec: SourceSpec, bronze_root: str, *, force: bool = False) -> str:
    """Télécharge une source vers le bronze et renvoie le chemin du fichier
    exploitable par les pipelines (le membre extrait si zip, le fichier sinon).
    """
    bronze_root = bronze_root.rstrip("/")
    dest = f"{bronze_root}/{spec.bronze_path}"

    if not spec.zip_member:
        return _download_with_fallback(spec, dest, force=force)

    member_dest = f"{bronze_root}/{spec.zip_member_bronze_path}"

    if not fetch.is_gcs_uri(dest):
        zip_path = _download_with_fallback(spec, dest, force=force)
        return fetch.extract_from_zip(zip_path, spec.zip_member, member_dest, force=force)

    # Bronze sur GCS : un seul téléchargement du zip en local, puis push de
    # l'archive ET du membre extrait.
    if not force and fetch.gcs_exists(dest) and fetch.gcs_exists(member_dest):
        return member_dest
    with tempfile.TemporaryDirectory() as tmpdir:
        local_zip = str(Path(tmpdir) / "archive.zip")
        local_member = str(Path(tmpdir) / "member")
        _download_with_fallback(spec, local_zip, force=True)
        fetch.extract_from_zip(local_zip, spec.zip_member, local_member, force=True)
        fetch.upload_to_gcs(local_zip, dest)
        fetch.upload_to_gcs(local_member, member_dest)
    return member_dest
