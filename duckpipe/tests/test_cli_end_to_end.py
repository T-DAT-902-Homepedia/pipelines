"""Répétition générale : tout le DAG via le CLI, en local, sur les vraies
données de exploration/data/raw/ (le bronze est constitué par symlinks, sans
re-téléchargement). C'est l'équivalent local exact de ce que Cloud Workflows
exécutera en prod, seules les racines de chemins changeant (ADR-0005).
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from duckpipe import export_web
from duckpipe.__main__ import main

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

RUN_DATE = "2026-07-02"
REF_N_COMMUNES_SCOREES = 17774
REF_N_COMMUNES = 34928
REF_N_DEPARTEMENTS = 109
# Fiches = contours ∪ communes scorées : 5 communes DVF fusionnées depuis
# (codes absents des contours 2025) + Saint-Martin (97127, hors Etalab).
REF_N_FICHES = REF_N_COMMUNES + 5

# fichier du cache exploration -> chemin bronze (layout ARCHITECTURE.md)
BRONZE_LAYOUT = {
    "dvf_full_2024.csv.gz": "dvf/year=2024/full.csv.gz",
    "dvf_full_2021.csv.gz": "dvf/year=2021/full.csv.gz",
    "communes-50m.geojson": "geom/communes-50m.geojson",
    "departements-50m.geojson": "geom/departements-50m.geojson",
    "communes-1000m.geojson": "geom/communes-1000m.geojson",
    "departements-100m.geojson": "geom/departements-100m.geojson",
    "departements-1000m.geojson": "geom/departements-1000m.geojson",
    "arrets_transport.csv": "transport/arrets_transport.csv",
    "revenus_commune_filosofi_2021.csv": "revenus/revenus_commune_filosofi_2021.csv",
    "catnat_gaspar.csv": "risques/catnat_gaspar.csv",
    "base-ic-logement-2021.CSV": "tourisme/base-ic-logement-2021.CSV",
    "securite_ssmsi_communale.csv.gz": "securite/securite_ssmsi_communale.csv.gz",
    "DS_BPE_2024_data.csv": "bpe/DS_BPE_2024_data.csv",
    "base_cc_emploi_pop_active_2021.CSV": "emploi/base_cc_emploi_pop_active_2021.CSV",
    "dpe_sample.jsonl": "dpe/dpe_sample.jsonl",
    "fiches_climatologiques_stations.csv": "climat/fiches_climatologiques_stations.csv",
    "communes-france-2024.csv": "proximite/communes-france-2024.csv",
}

# Ordre du DAG : geometries d'abord (les preprocess spatiaux en dépendent),
# puis les sources, puis score.
PIPELINE_ORDER = [
    "geometries",
    "geometries_web",
    "dvf",
    "transport",
    "revenus",
    "risques",
    "tourisme",
    "securite",
    "equipements",
    "emploi",
    "dpe",
    "climat",
    "proximite_metropole",
]


def test_full_dag_via_cli(tmp_path: Path) -> None:
    root = tmp_path / "data"
    for raw_name, bronze_path in BRONZE_LAYOUT.items():
        dest = root / "bronze" / bronze_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.symlink_to(EXPLORATION_RAW / raw_name)

    common = ["--env", "local", "--local-root", str(root), "--run-date", RUN_DATE]

    for pipeline_name in PIPELINE_ORDER:
        main(["run", pipeline_name, *common])

    # Millésime annexe pour l'évolution des prix (2022 absent : toléré).
    main(["run", "prix_millesime", "--env", "local", "--local-root", str(root),
          "--year", "2021", "--run-date", RUN_DATE])

    main(["validate-silver", *common])
    main(["run", "score", *common])
    main(["validate-gold", *common])
    main(["publish", *common])

    latest = root / "gold" / "score_territoire" / "latest" / "score.parquet"
    assert latest.exists()
    con = duckdb.connect(":memory:")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{latest}')").fetchone()[0]
    assert n == REF_N_COMMUNES_SCOREES

    assert (root / "gold" / "dq_reports" / f"silver_{RUN_DATE}.json").exists()
    assert (root / "gold" / "dq_reports" / f"gold_{RUN_DATE}.json").exists()

    # --- Artefacts web statiques (ADR-0013) --------------------------------
    main(["publish-web", *common])

    web = root / "web" / "v1"
    meta = json.loads((web / "meta.json").read_text())
    assert meta["base"] == f"runs/{RUN_DATE}"
    assert meta["nb_communes"] == REF_N_FICHES
    assert meta["nb_communes_scorees"] == REF_N_COMMUNES_SCOREES

    run_root = web / "runs" / RUN_DATE
    communes_mid_path = run_root / "choropleth" / "communes-mid.geojson"
    communes_mid = json.loads(communes_mid_path.read_text())
    assert len(communes_mid["features"]) == REF_N_COMMUNES
    sample = communes_mid["features"][0]["properties"]
    assert {"code_commune", "prix_m2_median", "maison_prix_m2_median", "score_valeur"} <= set(
        sample
    )
    assert {"gap", "dpe_dominant", "n_prix", "n_emploi"} <= set(sample)
    # Garde-fou : l'ajout des dimensions du score (ADR-0014) ne doit pas faire
    # déraper le poids de la choroplèthe nationale (30,3 Mo bruts mesurés,
    # 3,4 Mo gzippés servis).
    assert communes_mid_path.stat().st_size < 40_000_000

    depts_low = json.loads(
        (run_root / "choropleth" / "departements-low.geojson").read_text()
    )
    assert len(depts_low["features"]) == REF_N_DEPARTEMENTS

    high_files = list((run_root / "choropleth" / "communes-high").glob("*.geojson"))
    fiche_files = list((run_root / "communes").glob("*.json"))
    assert len(high_files) == len(fiche_files)
    assert len(fiche_files) > 90  # ~101 départements

    index = json.loads((run_root / "search" / "index.json").read_text())
    assert len(index) == REF_N_FICHES

    # Évolution : Bordeaux a des transactions en 2021 et 2024 (2022 non
    # publié). Pas Paris : ses transactions DVF sont codées par
    # arrondissement (751xx), 75056 n'a ni agrégat ni évolution — normal.
    fiches_33 = json.loads((run_root / "communes" / "33.json").read_text())
    bordeaux = next(f for f in fiches_33 if f["code_commune"] == "33063")
    assert [entry["annee"] for entry in bordeaux["evolution"]] == [2021, 2024]
    assert bordeaux["score"] is not None

    classement = json.loads((run_root / "classements" / "gap-pondere.json").read_text())
    assert len(classement) == 100
    assert classement[0]["rang"] == 1

    _assert_score_compat(web)


def _assert_score_compat(web: Path) -> None:
    """score.geojson de compatibilité (ADR-0014) : contrat de l'ancien
    webapp_export — communes scorées avec contours uniquement (les scorées
    hors contours, comptées dans REF_N_FICHES, en sont exclues), à la racine
    v1/ hors du run."""
    score = json.loads((web / "score.geojson").read_text())
    assert len(score["features"]) == REF_N_COMMUNES_SCOREES - (REF_N_FICHES - REF_N_COMMUNES)
    props = score["features"][0]["properties"]
    assert set(props) == {
        "code_commune",
        "nom",
        "dep",
        "prix",
        "nb_transactions",
        "dpe",
        "score_valeur",
        "gap",
        "gap_pondere",
        *export_web.SCORE_DIMENSIONS,
    }
