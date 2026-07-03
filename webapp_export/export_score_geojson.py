#!/usr/bin/env python3
"""Export du gold `score_territoire` (GCS) vers un GeoJSON publié pour la webapp.

Le bucket source `gs://homepedia-data` est privé : le navigateur ne peut pas lire
les parquet. On exporte donc un GeoJSON combiné (géométrie simplifiée + score +
12 dimensions) et on le **publie sur le bucket public `homepedia-web`** (public +
CORS), que le front récupère directement par HTTP comme un appel d'API.

Pré-requis : `gcloud` authentifié (lecture `homepedia-data`, écriture
`homepedia-web`) et duckdb avec l'extension spatial. Lancer depuis pipelines/ :

    cd webapp_export
    uv run --with duckdb python export_score_geojson.py

Régénérer après chaque run gold. Utiliser `--local <chemin>` pour écrire un
fichier au lieu de publier (debug).
"""

from __future__ import annotations

import argparse
import gzip
import json
import subprocess
import tempfile
from pathlib import Path

import duckdb

GOLD = "gs://homepedia-data/gold/score_territoire/latest/score.parquet"
GEOM = "gs://homepedia-data/silver/communes_geom/communes_geom.parquet"

# Destination publique (bucket homepedia-web, public + CORS, servi gzip).
DEST = "gs://homepedia-web/v1/score.geojson"

# Tolérance de simplification (degrés). 0.003 ≈ ~300 m : divise le poids par ~4
# (33 Mo brut -> ~8 Mo). Appliquée en simplification de COUVERTURE (topologique)
# et non par commune -> les bords partagés restent cohérents (pas de trous).
# Baisser (ex. 0.0015) pour des contours plus fins en zone dense (Paris), au prix
# d'un fichier plus lourd.
SIMPLIFY_TOL = 0.003

# 12 dimensions normalisées du score (0–1), reprises telles quelles dans les properties.
DIMENSIONS = [
    "n_prix", "n_transport", "n_access_fin", "n_risques", "n_tourisme",
    "n_securite", "n_services", "n_loisirs", "n_ensoleillement", "n_emploi",
    "n_proximite", "n_dpe",
]

# Correspondance INSEE département -> région. La simplification de couverture
# (ST_CoverageSimplify) charge tout le bloc traité en mémoire (~19 Ko/vertex) :
# la France entière (2.75M vertices) dépasse la RAM. On découpe donc PAR RÉGION,
# traitée séquentiellement (pic mémoire = plus grosse région, ~7 Go) : les bords
# partagés au sein d'une région restent cohérents (Paris + petite couronne étant
# tous en Île-de-France, leurs slivers inter-départements sont corrigés) ; seules
# d'éventuelles coutures fines subsistent le long des frontières entre régions.
_REGIONS = {
    "11": "75 77 78 91 92 93 94 95",
    "24": "18 28 36 37 41 45",
    "27": "21 25 39 58 70 71 89 90",
    "28": "14 27 50 61 76",
    "32": "02 59 60 62 80",
    "44": "08 10 51 52 54 55 57 67 68 88",
    "52": "44 49 53 72 85",
    "53": "22 29 35 56",
    "75": "16 17 19 23 24 33 40 47 64 79 86 87",
    "76": "09 11 12 30 31 32 34 46 48 65 66 81 82",
    "84": "01 03 07 15 26 38 42 43 63 69 73 74",
    "93": "04 05 06 13 83 84",
    "94": "2A 2B",
}
DEPT_REGION = {d: r for r, depts in _REGIONS.items() for d in depts.split()}


def _gcs_download(uri: str, dest: Path) -> None:
    # Pas de capture_output : on laisse gcloud écrire son propre stderr (message
    # d'auth/permission utile), sinon un échec ne remonte qu'un "exit status 1"
    # opaque au prochain run annuel.
    try:
        subprocess.run(["gcloud", "storage", "cp", uri, str(dest)], check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(
            f"[export] échec gcloud cp {uri} (exit {exc.returncode}). "
            "Vérifier l'auth : gcloud auth login"
        ) from exc


def build_geojson(gold: str, geom: str) -> str:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute(f"SET temp_directory = '{Path(tempfile.gettempdir()) / 'duckdb_score_spill'}';")
    dim_select = ", ".join(f"s.{d}" for d in DIMENSIONS)
    dim_props = ",\n".join(f"'{d}', round(o.{d}, 3)" for d in DIMENSIONS)

    # Correspondance département -> région, chargée en table pour le rattachement.
    con.execute("CREATE TEMP TABLE dept_region(dept VARCHAR, region VARCHAR)")
    con.executemany(
        "INSERT INTO dept_region VALUES (?, ?)", list(DEPT_REGION.items())
    )

    # Jointure sur code_commune (les communes scorées sans géométrie sont écartées
    # par le INNER JOIN) + rattachement région (fallback = département si non mappé,
    # DOM p.ex.). rn = index GLOBAL stable pour rejoindre les properties à la fin.
    con.execute(f"""
        CREATE TEMP TABLE ordered AS
        SELECT
            g.geom AS geom,
            g.code_commune AS code_commune,
            g.nom_commune AS nom_commune,
            s.code_departement, s.prix_m2_median, s.nb_transactions,
            s.dpe_dominant, s.score_valeur, s.gap, s.gap_pondere,
            {dim_select},
            coalesce(dr.region, s.code_departement) AS region,
            row_number() OVER (ORDER BY g.code_commune) AS rn
        FROM read_parquet('{gold}') s
        JOIN read_parquet('{geom}') g USING (code_commune)
        LEFT JOIN dept_region dr ON dr.dept = s.code_departement
    """)

    # Simplification de COUVERTURE topologique, RÉGION PAR RÉGION (séquentiel pour
    # borner la mémoire). Au sein d'une région, chaque frontière partagée est réduite
    # à l'identique des deux côtés -> aucun trou/sliver (contrairement à ST_Simplify
    # par commune). ST_Dump explose la collection (et les MultiPolygon des îles) ;
    # path[1] = index LOCAL au chunk -> on rejoint local_rn pour retrouver le rn
    # global ; ST_Collect réassemble les parts d'une commune (assemblage simple,
    # là où ST_Union_Agg dissoudrait — trop coûteux).
    con.execute("CREATE TEMP TABLE simplified(rn BIGINT, geom GEOMETRY)")
    regions = [r[0] for r in con.execute(
        "SELECT DISTINCT region FROM ordered ORDER BY region"
    ).fetchall()]
    for region in regions:
        con.execute(f"""
            INSERT INTO simplified
            WITH cur AS (
                SELECT geom, rn AS global_rn,
                       row_number() OVER (ORDER BY rn) AS local_rn
                FROM ordered WHERE region = ?
            ),
            coverage AS (
                SELECT ST_CoverageSimplify(array_agg(geom ORDER BY local_rn), {SIMPLIFY_TOL}) AS coll
                FROM cur
            ),
            dumped AS (
                SELECT d.path[1] AS local_rn, ST_Collect(list(d.geom)) AS geom
                FROM coverage, UNNEST(ST_Dump(coll)) AS u(d)
                GROUP BY d.path[1]
            )
            SELECT c.global_rn, dp.geom
            FROM dumped dp JOIN cur c USING (local_rn)
        """, [region])

    sql = f"""
        SELECT json_object(
            'type', 'FeatureCollection',
            'features', coalesce(json_group_array(json_object(
                'type', 'Feature',
                'geometry', CAST(ST_AsGeoJSON(sm.geom) AS JSON),
                'properties', json_object(
                    'code_commune', o.code_commune,
                    'nom', o.nom_commune,
                    'dep', o.code_departement,
                    'prix', round(o.prix_m2_median),
                    'nb_transactions', o.nb_transactions,
                    'dpe', o.dpe_dominant,
                    'score_valeur', round(o.score_valeur, 3),
                    'gap', round(o.gap, 3),
                    'gap_pondere', round(o.gap_pondere, 3),
                    {dim_props}
                )
            )), json_array())
        )::VARCHAR
        FROM ordered o JOIN simplified sm USING (rn)
    """
    return con.execute(sql).fetchone()[0]


def _publish(geojson: str, dest: str) -> None:
    """Gzip le GeoJSON et l'upload sur le bucket public avec les bons en-têtes
    (mêmes conventions que les objets voisins : geo+json, gzip, cache court)."""
    with tempfile.TemporaryDirectory() as tmp:
        gz = Path(tmp) / "score.geojson.gz"
        gz.write_bytes(gzip.compress(geojson.encode(), compresslevel=9))
        subprocess.run(
            [
                "gcloud", "storage", "cp", str(gz), dest,
                "--content-type=application/geo+json",
                "--content-encoding=gzip",
                "--cache-control=public, max-age=300",
            ],
            check=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--local",
        metavar="CHEMIN",
        help="écrire un fichier au lieu de publier sur le bucket (debug)",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        gold_local = Path(tmp) / "score.parquet"
        geom_local = Path(tmp) / "communes_geom.parquet"
        print(f"[export] téléchargement {GOLD}")
        _gcs_download(GOLD, gold_local)
        print(f"[export] téléchargement {GEOM}")
        _gcs_download(GEOM, geom_local)

        print("[export] jointure + simplification + GeoJSON...")
        geojson = build_geojson(str(gold_local), str(geom_local))

    n = len(json.loads(geojson)["features"])
    size = len(geojson) / 1e6
    if args.local:
        out = Path(args.local)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(geojson)
        print(f"[export] écrit {out} — {size:.1f} Mo, {n} communes")
    else:
        print(f"[export] publication -> {DEST} ({size:.1f} Mo brut, {n} communes)")
        _publish(geojson, DEST)
        print("[export] publié.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
