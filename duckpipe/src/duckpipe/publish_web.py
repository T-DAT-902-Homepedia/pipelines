"""Publication des artefacts web statiques (cf. ADR-0013).

Comme validation.py, ce n'est pas une transformation table -> table mais une
étape d'orchestration appelée par le CLI : charge les tables silver/gold via
le catalog, matérialise les tables web (export_web), écrit les fichiers
GeoJSON/JSON en staging local, puis les uploade gzippés sur le bucket public
avec les bons Content-Type/Cache-Control. meta.json est écrit EN DERNIER :
le swap de run est atomique du point de vue de la webapp.
"""

from __future__ import annotations

import gzip
import json
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe import catalogs, export_web, fetch
from duckpipe.datasets.base import DatasetError

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)

# Précision GDAL (décimales) par artefact : ~11 m à 4 décimales.
PRECISION_LOW = 2
PRECISION_MID = 3
PRECISION_HIGH = 4

CACHE_IMMUTABLE = "public, max-age=31536000, immutable"
CACHE_META = "public, max-age=300"

FICHE_TABLES = [
    "revenus",
    "emploi",
    "commune_transport",
    "equipements",
    "securite",
    "tourisme",
    "risques",
    "climat",
    "proximite_metropole",
]


def _copy_geojson(
    con: duckdb.DuckDBPyConnection, query: str, dest: Path, *, precision: int
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({query}) TO '{dest}' (FORMAT GDAL, DRIVER 'GeoJSON', "
        f"LAYER_CREATION_OPTIONS 'COORDINATE_PRECISION={precision}')"
    )


def _copy_json(con: duckdb.DuckDBPyConnection, query: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO '{dest}' (FORMAT JSON, ARRAY true)")


def _load_tables(
    con: duckdb.DuckDBPyConnection, catalog, year: int
) -> tuple[dict[int, str], bool]:
    """Charge toutes les tables nécessaires ; renvoie (millésimes disponibles,
    présence des géométries régionales)."""
    for table in [
        "commune_geom",
        "commune_geom_1000m",
        "dept_geom_100m",
        "dept_geom_1000m",
        "commune_agg",
        "commune_agg_type",
        "dept_agg",
        "score_territoire",
        "dvf",
        *FICHE_TABLES,
    ]:
        catalog.load(con, table)

    # Régions : silver apparu après les premiers runs — toléré absent (les
    # artefacts régionaux sont alors simplement omis du run publié).
    has_regions = True
    try:
        catalog.load(con, "region_geom_1000m")
    except DatasetError:
        logger.warning("[warn] region_geom_1000m absent, export web sans maille régionale")
        has_regions = False

    millesimes: dict[int, str] = {}
    for annee in catalogs.WEB_MILLESIMES:
        if annee == year:
            continue
        table = f"commune_prix_{annee}"
        try:
            catalog.load(con, table)
            millesimes[annee] = table
        except DatasetError:
            logger.warning("[warn] millésime %s absent, évolution sans cette année", annee)

    # Avis : optionnels (étape NLP externe, couverture partielle). Si absents,
    # on matérialise une table vide typée pour que fiches/exports se construisent
    # quand même (fiche.avis = null, aucun artefact avis écrit).
    try:
        catalog.load(con, "avis_commune")
    except DatasetError:
        logger.warning("[warn] avis_commune absent, export web sans analyse d'avis")
        export_web.create_avis_stub(con)

    return millesimes, has_regions


def _write_payload(payload: dict, dest: Path) -> None:
    """Écrit un payload charts/ (enveloppe JSON, pas une table COPY)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _generate_artifacts(
    con: duckdb.DuckDBPyConnection,
    staging: Path,
    year: int,
    millesimes: dict[int, str],
    *,
    has_regions: bool,
) -> None:
    """Matérialise les tables web et écrit tous les fichiers sous `staging`.

    Les niveaux de détail sont portés par les contours pré-simplifiés Etalab
    (variantes 1000m/100m/50m chargées en silver), pas par une simplification
    calculée (cf. note dans export_web.py).
    """
    if has_regions:
        export_web.build_choropleth_regions(
            con, "region_geom_1000m", "dvf", "score_territoire"
        )
        _copy_geojson(
            con,
            "SELECT * FROM web_choropleth_regions",
            staging / "choropleth" / "regions-low.geojson",
            precision=PRECISION_LOW,
        )
        _copy_json(
            con,
            "SELECT * EXCLUDE (geom) FROM web_choropleth_regions ORDER BY code_region",
            staging / "stats" / "regions.json",
        )

    for lod, geom_table, precision in [
        ("low", "dept_geom_1000m", PRECISION_LOW),
        ("mid", "dept_geom_100m", PRECISION_MID),
    ]:
        table = export_web.build_choropleth_departements(
            con,
            geom_table,
            "dept_agg",
            "score_territoire",
            out_table=f"web_choropleth_departements_{lod}",
        )
        _copy_geojson(
            con,
            f"SELECT * FROM {table}",
            staging / "choropleth" / f"departements-{lod}.geojson",
            precision=precision,
        )

    export_web.build_choropleth_communes(
        con,
        "commune_geom_1000m",
        "commune_agg",
        "commune_agg_type",
        "score_territoire",
        out_table="web_choropleth_communes_mid",
    )
    _copy_geojson(
        con,
        "SELECT * FROM web_choropleth_communes_mid",
        staging / "choropleth" / "communes-mid.geojson",
        precision=PRECISION_MID,
    )

    export_web.build_choropleth_communes(
        con,
        "commune_geom",
        "commune_agg",
        "commune_agg_type",
        "score_territoire",
        out_table="web_choropleth_communes_high",
    )
    departements = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT code_departement FROM web_choropleth_communes_high ORDER BY 1"
        ).fetchall()
    ]
    for dept in departements:
        _copy_geojson(
            con,
            f"SELECT * FROM web_choropleth_communes_high WHERE code_departement = '{dept}'",
            staging / "choropleth" / "communes-high" / f"{dept}.geojson",
            precision=PRECISION_HIGH,
        )

    export_web.build_evolution(con, "commune_agg", year, millesimes)
    export_web.build_fiches(
        con,
        "commune_geom",
        "commune_agg",
        "commune_agg_type",
        "score_territoire",
        "web_evolution",
        *FICHE_TABLES,
        "avis_commune",
    )
    for dept in departements:
        _copy_json(
            con,
            f"SELECT * FROM web_fiches WHERE code_departement = '{dept}' ORDER BY code_commune",
            staging / "communes" / f"{dept}.json",
        )

    # Analyse d'avis : un fichier par département présent (aligné sur communes/).
    # Vide (stub) → aucun fichier, la webapp feature-gate via meta.schema_version.
    export_web.build_avis(con, "avis_commune")
    avis_departements = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT code_departement FROM web_avis "
            "WHERE code_departement IS NOT NULL ORDER BY 1"
        ).fetchall()
    ]
    for dept in avis_departements:
        _copy_json(
            con,
            f"SELECT * FROM web_avis WHERE code_departement = '{dept}' ORDER BY code_commune",
            staging / "avis" / f"{dept}.json",
        )

    export_web.build_search_index(con, "commune_geom", "commune_agg", "score_territoire")
    _copy_json(con, "SELECT * FROM web_search_index", staging / "search" / "index.json")

    export_web.build_classement(con, "score_territoire")
    _copy_json(
        con,
        "SELECT * FROM web_classement ORDER BY rang",
        staging / "classements" / "gap-pondere.json",
    )

    # Échantillon de mutations pour la heatmap (chargé lazy côté front).
    export_web.build_points_sample(con, "dvf")
    _copy_json(
        con,
        "SELECT * FROM web_points_sample",
        staging / "points" / "transactions-sample.json",
    )

    # Artefacts charts/ (contrats de webapp/src/lib/charts.ts).
    _write_payload(
        export_web.build_stats_communes(
            con,
            "score_territoire",
            "revenus",
            "emploi",
            "climat",
            "tourisme",
            "proximite_metropole",
            "commune_transport",
            year=year,
        ),
        staging / "charts" / "stats_communes.json",
    )
    _write_payload(
        export_web.build_prix_distribution(con, "dvf", year=year),
        staging / "charts" / "prix_distribution.json",
    )
    _write_payload(
        export_web.build_prix_series(con, "commune_agg", year, millesimes),
        staging / "charts" / "prix_series.json",
    )


def _build_meta(con: duckdb.DuckDBPyConnection, year: int, run_date: str) -> dict:
    # schema_version reste à 1 : tous les ajouts (avis, régions, charts,
    # points) sont ADDITIFS — le front déployé valide `z.literal(1)` et
    # casserait sur un bump. La webapp feature-gate l'analyse d'avis sur
    # `nb_communes_avis` (0 ou champ absent = section masquée) ; réserver
    # l'incrément aux changements de contrat non rétrocompatibles.
    return {
        "schema_version": 1,
        "run_date": run_date,
        "year": year,
        "base": f"runs/{run_date}",
        "nb_communes": con.execute("SELECT count(*) FROM web_fiches").fetchone()[0],
        "nb_communes_scorees": con.execute(
            "SELECT count(*) FROM web_fiches WHERE score IS NOT NULL"
        ).fetchone()[0],
        "nb_communes_avis": con.execute("SELECT count(*) FROM web_avis").fetchone()[0],
        "generated_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
    }


def _content_type(path: Path) -> str:
    return "application/geo+json" if path.suffix == ".geojson" else "application/json"


def _upload_asset(local_path: Path, gcs_uri: str, *, cache_control: str) -> None:
    """Upload gzippé avec métadonnées HTTP (l'upload_to_gcs de fetch.py ne
    gère pas content_encoding/cache_control, helper dédié ici).

    Le content_type DOIT passer par le paramètre d'upload_from_string :
    le fixer en attribut de blob entre en conflit avec le Content-Type
    par défaut de l'upload (text/plain) et GCS rejette en 400.
    """
    blob = fetch._gcs_blob(gcs_uri)
    blob.content_encoding = "gzip"
    blob.cache_control = cache_control
    blob.upload_from_string(
        gzip.compress(local_path.read_bytes(), compresslevel=9),
        content_type=_content_type(local_path),
    )


def publish_web(con: duckdb.DuckDBPyConnection, env, *, year: int, run_date: str) -> None:
    """Génère et publie tous les artefacts web du run.

    En environnement local, les fichiers sont écrits directement sous
    `<root>/web/v1/` (pas d'upload) ; en prod, staging temporaire puis upload
    gzippé vers le bucket public, meta.json en dernier.
    """
    catalog = catalogs.build_catalog(env, year=year, run_date=run_date)
    millesimes, has_regions = _load_tables(con, catalog, year)

    run_root = catalogs.web_run_root(env, run_date)
    is_remote = fetch.is_gcs_uri(run_root)

    if not is_remote:
        staging = Path(run_root)
        _generate_artifacts(con, staging, year, millesimes, has_regions=has_regions)
        export_web.build_score_geojson_compat(con, "web_choropleth_communes_mid")
        _copy_geojson(
            con,
            "SELECT * FROM web_score_compat",
            Path(catalogs.web_root(env)) / "score.geojson",
            precision=PRECISION_MID,
        )
        meta = _build_meta(con, year, run_date)
        meta_path = Path(catalogs.web_root(env)) / "meta.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("[ok] artefacts web -> %s", staging)
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        staging = Path(tmpdir)
        _generate_artifacts(con, staging, year, millesimes, has_regions=has_regions)
        meta = _build_meta(con, year, run_date)

        files = sorted(path for path in staging.rglob("*") if path.is_file())
        logger.info("[upload] %d artefacts vers %s", len(files), run_root)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(
                    _upload_asset,
                    path,
                    f"{run_root}/{path.relative_to(staging)}",
                    cache_control=CACHE_IMMUTABLE,
                )
                for path in files
            ]
            for future in futures:
                future.result()

        # score.geojson de compatibilité (déprécié, cf. ADR-0014) : muté en
        # place à la racine v1/, hors du run immuable (écrit dans staging
        # APRÈS la collecte rglob), cache court comme meta.json.
        export_web.build_score_geojson_compat(con, "web_choropleth_communes_mid")
        compat_local = staging / "score.geojson"
        _copy_geojson(
            con, "SELECT * FROM web_score_compat", compat_local, precision=PRECISION_MID
        )
        _upload_asset(
            compat_local, f"{catalogs.web_root(env)}/score.geojson", cache_control=CACHE_META
        )

        # meta.json en dernier : le run devient visible atomiquement.
        meta_local = staging / "meta.json"
        meta_local.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
        _upload_asset(
            meta_local, f"{catalogs.web_root(env)}/meta.json", cache_control=CACHE_META
        )
    logger.info("[ok] run web publié : %s", run_root)
