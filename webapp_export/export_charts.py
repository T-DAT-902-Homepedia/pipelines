#!/usr/bin/env python3
"""Export des datasets « graphiques » (#17) vers le CDN webapp.

Produit trois artefacts JSON sous `{base}/charts/` (base = run courant du
meta.json, donc AUCUNE mutation de meta.json) :

- `stats_communes.json`  : valeurs brutes par commune (chômage, revenu,
  climat, tourisme, distance métropole, transport) depuis les silvers.
- `prix_distribution.json` : histogrammes du prix au m² (tous/maison/appart)
  précalculés depuis le silver DVF du millésime courant.
- `prix_series.json`     : médianes annuelles du prix au m² par commune
  (+ série nationale), recalculées depuis les CSV DVF publics de data.gouv
  — mêmes règles de nettoyage que `duckpipe/pipelines/dvf.py::ingest_dvf`,
  appliquées à l'identique sur chaque millésime pour une série cohérente.

Pré-requis : `gcloud` authentifié (lecture `homepedia-data`). Les CSV DVF
(~500 Mo/an) sont mis en cache local (--cache) pour ne pas re-télécharger.

    cd webapp_export
    uv run --with duckdb python export_charts.py --local ../../webapp/public/local-data

`--local <dir>` écrit l'arborescence CDN (meta.json copié du bucket public +
artefacts) pour tester la webapp avec VITE_DATA_URL. Sans --local, `--publish`
envoie sur gs://homepedia-web (même convention gzip que export_score_geojson).
"""

from __future__ import annotations

import argparse
import gzip
import json
import subprocess
import tempfile
import urllib.request
from pathlib import Path

import duckdb

from export_score_geojson import _gcs_download

GOLD = "gs://homepedia-data/gold/score_territoire/latest/score.parquet"
SILVER = "gs://homepedia-data/silver"

# Silvers porteurs des valeurs brutes (cf. duckpipe/catalogs.py::SILVER_PATHS).
SILVERS = {
    "emploi": f"{SILVER}/emploi_commune/emploi.parquet",
    "revenus": f"{SILVER}/revenus_commune/revenus.parquet",
    "climat": f"{SILVER}/climat_commune/climat.parquet",
    "tourisme": f"{SILVER}/tourisme_commune/tourisme.parquet",
    "proximite": f"{SILVER}/proximite_commune/proximite_metropole.parquet",
    "transport": f"{SILVER}/transport_commune/transport_commune.parquet",
}
DVF_SILVER_YEAR = 2024
DVF_SILVER = f"{SILVER}/dvf_clean/year={DVF_SILVER_YEAR}/dvf.parquet"

META_URL = "https://storage.googleapis.com/homepedia-web/v1/meta.json"
DEST_ROOT = "gs://homepedia-web/v1"

DVF_URL_TEMPLATE = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/full.csv.gz"

# Seuils de plausibilité — repris de duckpipe/pipelines/dvf.py (mêmes valeurs).
PRIX_M2_MIN = 100
PRIX_M2_MAX = 50_000
SURFACE_MIN = 9
SURFACE_MAX = 1_000
# Médiane annuelle publiée seulement si assez de ventes (même seuil que `fiable`).
MIN_TRANSACTIONS_SERIE = 5

HIST_BINS = 60


# --------------------------------------------------------------------------- #
# Dataset 1 : stats brutes par commune
# --------------------------------------------------------------------------- #
def build_stats(con: duckdb.DuckDBPyConnection, gold: str, silvers: dict[str, str]) -> dict:
    """Périmètre = communes scorées (gold), LEFT JOIN des silvers par code_commune."""
    rows = con.execute(f"""
        SELECT
            s.code_commune,
            r.nom_commune                                   AS nom,
            s.code_departement                              AS dep,
            round(s.prix_m2_median)                         AS prix_m2_median,
            s.nb_transactions,
            round(e.taux_chomage, 2)                        AS taux_chomage,
            round(e.taux_couverture_emploi, 2)              AS taux_couverture_emploi,
            round(r.revenu_median)                          AS revenu_median,
            round(c.ensoleillement_h_an)                    AS ensoleillement_h_an,
            round(c.jours_ensoleilles)                      AS jours_ensoleilles,
            round(c.temperature_moy_annuelle, 1)            AS temperature_moy_annuelle,
            round(t.part_residences_secondaires, 4)         AS part_residences_secondaires,
            round(p.dist_metropole_km, 1)                   AS dist_metropole_km,
            tr.nb_arrets,
            round(tr.densite_arrets_km2, 2)                 AS densite_arrets_km2
        FROM read_parquet('{gold}') s
        LEFT JOIN read_parquet('{silvers["emploi"]}')    e  USING (code_commune)
        LEFT JOIN read_parquet('{silvers["revenus"]}')   r  USING (code_commune)
        LEFT JOIN read_parquet('{silvers["climat"]}')    c  USING (code_commune)
        LEFT JOIN read_parquet('{silvers["tourisme"]}')  t  USING (code_commune)
        LEFT JOIN read_parquet('{silvers["proximite"]}') p  USING (code_commune)
        LEFT JOIN read_parquet('{silvers["transport"]}') tr USING (code_commune)
        ORDER BY s.code_commune
    """).fetchall()
    cols = [d[0] for d in con.description]
    return {
        "schema_version": 1,
        "year": DVF_SILVER_YEAR,
        "communes": [dict(zip(cols, row)) for row in rows],
    }


# --------------------------------------------------------------------------- #
# Dataset 2 : distribution des prix au m² (histogrammes précalculés)
# --------------------------------------------------------------------------- #
def build_distribution(con: duckdb.DuckDBPyConnection, dvf: str) -> dict:
    """Bins uniformes de 0 au p99 global (le silver est déjà clippé p1-p99
    par département×type, on borne juste l'axe pour des bins lisibles)."""
    p99 = con.execute(
        f"SELECT quantile_cont(prix_m2, 0.99) FROM read_parquet('{dvf}')"
    ).fetchone()[0]
    hi = float(round(p99, -2))  # arrondi à la centaine pour des bords propres
    width = hi / HIST_BINS

    def counts(where: str) -> list[int]:
        rows = con.execute(f"""
            SELECT least(floor(prix_m2 / {width}), {HIST_BINS - 1})::INT AS b, count(*)
            FROM read_parquet('{dvf}')
            WHERE prix_m2 <= {hi} {where}
            GROUP BY b ORDER BY b
        """).fetchall()
        out = [0] * HIST_BINS
        for b, n in rows:
            out[b] = n
        return out

    return {
        "schema_version": 1,
        "year": DVF_SILVER_YEAR,
        "bin_edges": [round(i * width) for i in range(HIST_BINS + 1)],
        "series": {
            "tous": counts(""),
            "maison": counts("AND type_local = 'Maison'"),
            "appartement": counts("AND type_local = 'Appartement'"),
        },
    }


# --------------------------------------------------------------------------- #
# Dataset 3 : séries annuelles de prix (CSV DVF publics)
# --------------------------------------------------------------------------- #
def _download_dvf_year(year: int, cache: Path) -> Path:
    dest = cache / f"dvf_{year}_full.csv.gz"
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[charts]   {year} : cache ({dest.stat().st_size / 1e6:.0f} Mo)")
        return dest
    url = DVF_URL_TEMPLATE.format(year=year)
    print(f"[charts]   {year} : téléchargement {url}")
    tmp = dest.with_suffix(".part")
    urllib.request.urlretrieve(url, tmp)
    tmp.rename(dest)
    return dest


def _clean_year(con: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    """Réplique ingest_dvf (duckpipe/pipelines/dvf.py) : filtre Vente
    maison/appart, dédup mono-bien par mutation, bornes absolues, clipping
    p1-p99 par département×type. Produit la table temporaire `dvf_year`."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dvf_lignes AS
        SELECT
            id_mutation,
            type_local,
            TRY_CAST(valeur_fonciere AS DOUBLE) AS valeur_fonciere,
            TRY_CAST(surface_reelle_bati AS DOUBLE) AS surface_bati,
            lpad(CAST(code_commune AS VARCHAR), 5, '0') AS code_commune,
            lpad(CAST(code_departement AS VARCHAR), 2, '0') AS code_departement,
            TRY_CAST(longitude AS DOUBLE) AS longitude,
            TRY_CAST(latitude AS DOUBLE) AS latitude
        FROM read_csv('{csv_path}', all_varchar=true, ignore_errors=true)
        WHERE nature_mutation = 'Vente'
          AND type_local IN ('Maison', 'Appartement')
          AND TRY_CAST(surface_reelle_bati AS DOUBLE) > 0
          AND TRY_CAST(valeur_fonciere AS DOUBLE) > 0
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dvf_dedup AS
        WITH par_mutation AS (
            SELECT id_mutation,
                   sum(surface_bati) AS surface_totale,
                   any_value(type_local) AS type_local,
                   any_value(valeur_fonciere) AS valeur_fonciere,
                   any_value(code_commune) AS code_commune,
                   any_value(code_departement) AS code_departement,
                   any_value(longitude) AS longitude,
                   any_value(latitude) AS latitude
            FROM dvf_lignes
            GROUP BY id_mutation
            HAVING count(DISTINCT valeur_fonciere) = 1
               AND count(DISTINCT type_local) = 1
        )
        SELECT code_commune, code_departement, type_local,
               valeur_fonciere / NULLIF(surface_totale, 0) AS prix_m2,
               surface_totale AS surface_bati, longitude, latitude
        FROM par_mutation
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dvf_bornes AS
        SELECT * FROM dvf_dedup
        WHERE surface_bati BETWEEN {SURFACE_MIN} AND {SURFACE_MAX}
          AND longitude IS NOT NULL AND latitude IS NOT NULL
          AND prix_m2 BETWEEN {PRIX_M2_MIN} AND {PRIX_M2_MAX}
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE dvf_year AS
        WITH bounds AS (
            SELECT code_departement, type_local,
                   quantile_cont(prix_m2, 0.01) AS p_lo,
                   quantile_cont(prix_m2, 0.99) AS p_hi
            FROM dvf_bornes GROUP BY code_departement, type_local
        )
        SELECT c.code_commune, c.prix_m2
        FROM dvf_bornes c
        JOIN bounds b USING (code_departement, type_local)
        WHERE c.prix_m2 BETWEEN b.p_lo AND b.p_hi
    """)


def build_series(con: duckdb.DuckDBPyConnection, years: list[int], cache: Path) -> dict:
    con.execute("CREATE OR REPLACE TABLE serie(code_commune VARCHAR, year INT, med DOUBLE, nb BIGINT)")
    con.execute("CREATE OR REPLACE TABLE serie_nat(year INT, med DOUBLE)")
    for year in years:
        csv_path = _download_dvf_year(year, cache)
        print(f"[charts]   {year} : nettoyage + agrégation")
        _clean_year(con, csv_path)
        con.execute(f"""
            INSERT INTO serie
            SELECT code_commune, {year}, median(prix_m2), count(*)
            FROM dvf_year GROUP BY code_commune
        """)
        con.execute(f"INSERT INTO serie_nat SELECT {year}, median(prix_m2) FROM dvf_year")

    national = {
        y: round(m) for y, m in con.execute("SELECT year, med FROM serie_nat").fetchall()
    }
    # Une entrée par commune : liste alignée sur `years`, null si < MIN ventes.
    rows = con.execute(f"""
        SELECT code_commune, year, round(med)
        FROM serie WHERE nb >= {MIN_TRANSACTIONS_SERIE}
    """).fetchall()
    idx = {y: i for i, y in enumerate(years)}
    communes: dict[str, list[float | None]] = {}
    for code, year, med in rows:
        communes.setdefault(code, [None] * len(years))[idx[year]] = med

    return {
        "schema_version": 1,
        "years": years,
        "national": [national.get(y) for y in years],
        "communes": communes,
    }


# --------------------------------------------------------------------------- #
# Écriture / publication
# --------------------------------------------------------------------------- #
def _fetch_meta() -> dict:
    with urllib.request.urlopen(META_URL) as res:
        return json.loads(res.read())


def _publish_json(payload: str, dest: str) -> None:
    """Comme export_score_geojson._publish, avec le content-type JSON."""
    with tempfile.TemporaryDirectory() as tmp:
        gz = Path(tmp) / "payload.json.gz"
        gz.write_bytes(gzip.compress(payload.encode(), compresslevel=9))
        subprocess.run(
            [
                "gcloud", "storage", "cp", str(gz), dest,
                "--content-type=application/json",
                "--content-encoding=gzip",
                "--cache-control=public, max-age=300",
            ],
            check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", metavar="DIR",
                        help="écrire l'arborescence CDN localement (test webapp)")
    parser.add_argument("--publish", action="store_true",
                        help="publier sur gs://homepedia-web (après validation)")
    # geo-dvf ne conserve que les 5 derniers millésimes (les plus anciens 404).
    parser.add_argument("--years", default="2021-2025",
                        help="plage de millésimes DVF pour les séries (ex. 2021-2025)")
    parser.add_argument("--cache", default=str(Path.home() / ".cache" / "homepedia-dvf"),
                        help="cache des CSV DVF téléchargés")
    args = parser.parse_args()
    if not args.local and not args.publish:
        parser.error("préciser --local <dir> (test) ou --publish")

    lo, _, hi = args.years.partition("-")
    years = list(range(int(lo), int(hi or lo) + 1))
    cache = Path(args.cache)
    cache.mkdir(parents=True, exist_ok=True)

    meta = _fetch_meta()
    base = meta["base"]
    print(f"[charts] run courant : base={base}")

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false;")

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        print("[charts] téléchargement gold + silvers…")
        gold_local = tmpdir / "score.parquet"
        _gcs_download(GOLD, gold_local)
        silvers_local: dict[str, str] = {}
        for name, uri in SILVERS.items():
            dest = tmpdir / f"{name}.parquet"
            _gcs_download(uri, dest)
            silvers_local[name] = str(dest)
        dvf_local = tmpdir / "dvf.parquet"
        _gcs_download(DVF_SILVER, dvf_local)

        print("[charts] stats_communes…")
        stats = build_stats(con, str(gold_local), silvers_local)
        print(f"[charts]   {len(stats['communes'])} communes")

        print("[charts] prix_distribution…")
        distribution = build_distribution(con, str(dvf_local))

    print(f"[charts] prix_series {years[0]}-{years[-1]}…")
    series = build_series(con, years, cache)
    print(f"[charts]   {len(series['communes'])} communes avec série")

    artifacts = {
        "charts/stats_communes.json": stats,
        "charts/prix_distribution.json": distribution,
        "charts/prix_series.json": series,
    }

    if args.local:
        root = Path(args.local)
        (root / base / "charts").mkdir(parents=True, exist_ok=True)
        (root / "meta.json").write_text(json.dumps(meta))
        for path, payload in artifacts.items():
            out = root / base / path
            out.write_text(json.dumps(payload, ensure_ascii=False))
            print(f"[charts] écrit {out} ({out.stat().st_size / 1e6:.1f} Mo)")
    if args.publish:
        for path, payload in artifacts.items():
            dest = f"{DEST_ROOT}/{base}/{path}"
            print(f"[charts] publication -> {dest}")
            _publish_json(json.dumps(payload, ensure_ascii=False), dest)
        print("[charts] publié.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
