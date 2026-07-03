#!/usr/bin/env python3
"""Export du gold `score_territoire` (GCS) vers un GeoJSON statique pour la webapp.

Le bucket `gs://homepedia-data` est privé et sans CORS : le navigateur ne peut pas
lire les parquet. On exporte donc, hors ligne, un GeoJSON combiné (géométrie
simplifiée + score + 12 dimensions) que la webapp sert en statique via Firebase
Hosting et charge par un simple `fetch('/data/score.geojson')`.

Pré-requis : `gcloud` authentifié (lecture de gs://homepedia-data) et duckdb avec
l'extension spatial (fournie par l'env duckpipe). Lancer depuis pipelines/ :

    cd webapp_export
    uv run --with duckdb python export_score_geojson.py \
        --out ../../webapp/public/data/score.geojson

Cadence ~annuelle (le pipeline tourne @yearly) : régénérer après chaque run gold.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

import duckdb

GOLD = "gs://homepedia-data/gold/score_territoire/latest/score.parquet"
GEOM = "gs://homepedia-data/silver/communes_geom/communes_geom.parquet"

# Simplification Douglas-Peucker (degrés). 0.003 ≈ ~300 m : suffisant à l'échelle
# France/région, divise le poids par ~4 (33 Mo brut -> ~8 Mo) sans artefact visible.
SIMPLIFY_TOL = 0.003

# 12 dimensions normalisées du score (0–1), reprises telles quelles dans les properties.
DIMENSIONS = [
    "n_prix", "n_transport", "n_access_fin", "n_risques", "n_tourisme",
    "n_securite", "n_services", "n_loisirs", "n_ensoleillement", "n_emploi",
    "n_proximite", "n_dpe",
]


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
    dims = ",\n".join(f"'{d}', round(s.{d}, 3)" for d in DIMENSIONS)
    # Jointure sur code_commune ; les 5 communes scorées sans géométrie sont
    # écartées par le INNER JOIN. geom simplifiée puis sérialisée en GeoJSON.
    sql = f"""
        SELECT json_object(
            'type', 'FeatureCollection',
            'features', coalesce(json_group_array(json_object(
                'type', 'Feature',
                'geometry', CAST(ST_AsGeoJSON(ST_Simplify(g.geom, {SIMPLIFY_TOL})) AS JSON),
                'properties', json_object(
                    'code_commune', g.code_commune,
                    'nom', g.nom_commune,
                    'dep', s.code_departement,
                    'prix', round(s.prix_m2_median),
                    'nb_transactions', s.nb_transactions,
                    'dpe', s.dpe_dominant,
                    'score_valeur', round(s.score_valeur, 3),
                    'gap', round(s.gap, 3),
                    'gap_pondere', round(s.gap_pondere, 3),
                    {dims}
                )
            )), json_array())
        )::VARCHAR
        FROM read_parquet('{gold}') s
        JOIN read_parquet('{geom}') g USING (code_commune)
    """
    return con.execute(sql).fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        # Ancré sur l'emplacement du script (pas le cwd) : lancé depuis n'importe
        # où, le fichier atterrit toujours dans webapp/public/data/.
        default=str(Path(__file__).resolve().parents[2] / "webapp/public/data/score.geojson"),
        help="chemin de sortie du GeoJSON (défaut : webapp/public/data/score.geojson)",
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

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(geojson)
    n = len(json.loads(geojson)["features"])
    print(f"[export] écrit {out} — {len(geojson) / 1e6:.1f} Mo, {n} communes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
