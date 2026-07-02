"""Répétition générale : tout le DAG via le CLI, en local, sur les vraies
données de exploration/data/raw/ (le bronze est constitué par symlinks, sans
re-téléchargement). C'est l'équivalent local exact de ce que Cloud Workflows
exécutera en prod, seules les racines de chemins changeant (ADR-0005).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from duckpipe.__main__ import main

EXPLORATION_RAW = Path(__file__).parents[2].parent / "exploration" / "data" / "raw"

pytestmark = pytest.mark.skipif(
    not EXPLORATION_RAW.exists(), reason="exploration/data/raw introuvable en local"
)

RUN_DATE = "2026-07-02"
REF_N_COMMUNES_SCOREES = 17774

# fichier du cache exploration -> chemin bronze (layout ARCHITECTURE.md)
BRONZE_LAYOUT = {
    "dvf_full_2024.csv.gz": "dvf/year=2024/full.csv.gz",
    "communes-50m.geojson": "geom/communes-50m.geojson",
    "departements-50m.geojson": "geom/departements-50m.geojson",
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
