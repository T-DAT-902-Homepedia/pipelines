"""Vérifie le node score et validate_gold contre la base de référence.

La fidélité numérique fine (écart max 2,22e-16 vs la réimplémentation pandas
de section_score) a été vérifiée par script de comparaison dédié ; ici on
valide les propriétés structurelles reproductibles sans pandas.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from duckpipe import validation
from duckpipe.pipelines.score import POIDS, score

REF_DB = Path(__file__).parents[2].parent / "exploration" / "data" / "exploration.duckdb"

pytestmark = pytest.mark.skipif(not REF_DB.exists(), reason="base de référence introuvable")

SCORE_INPUT_TABLES = [
    "commune_agg",
    "commune_transport",
    "dpe",
    "revenus",
    "risques",
    "tourisme",
    "securite",
    "equipements",
    "climat",
    "emploi",
    "proximite_metropole",
]

# Nombre de communes fiables observé dans la base de référence.
REF_N_COMMUNES_FIABLES = 17774


@pytest.fixture
def con_ref():
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{REF_DB}' AS ref (READ_ONLY)")
    for table in SCORE_INPUT_TABLES:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM ref.{table}")
    yield con
    con.close()


def test_poids_score_somme_a_un() -> None:
    assert sum(POIDS.values()) == pytest.approx(1.0)


def test_score_on_reference_data(con_ref) -> None:
    result_table = score(con_ref, *SCORE_INPUT_TABLES)

    n = con_ref.execute(f"SELECT count(*) FROM {result_table}").fetchone()[0]
    assert n == REF_N_COMMUNES_FIABLES

    n_null = con_ref.execute(
        f"SELECT count(*) FROM {result_table} WHERE score_valeur IS NULL"
    ).fetchone()[0]
    assert n_null == 0

    hors_bornes = con_ref.execute(
        f"SELECT count(*) FROM {result_table} "
        f"WHERE score_valeur < 0 OR score_valeur > 1 OR gap < -1 OR gap > 1"
    ).fetchone()[0]
    assert hors_bornes == 0


def test_validate_gold_on_reference_score(con_ref) -> None:
    score(con_ref, *SCORE_INPUT_TABLES)

    report = validation.validate_gold(con_ref)
    assert report["nb_communes_scorees"] == REF_N_COMMUNES_FIABLES

    # Stabilité vs soi-même : tau = 1, la validation passe.
    report2 = validation.validate_gold(con_ref, previous_top=report["top_25"])
    assert report2["kendall_tau_top25"] == 1.0
