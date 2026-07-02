"""Vérifie les agrégats commune_agg_type et dept_agg contre la table dvf de
la base de référence (comptes établis directement sur cette table).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from duckpipe.pipelines.dvf import commune_agg_type, dept_agg

REF_DB = Path(__file__).parents[2].parent / "exploration" / "data" / "exploration.duckdb"

pytestmark = pytest.mark.skipif(not REF_DB.exists(), reason="base de référence introuvable")

# Comptes observés sur la table dvf de référence (747 201 transactions 2024).
REF_COMMUNE_TYPE_ROWS = 38970
REF_DEPTS = 94


@pytest.fixture
def con_ref():
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{REF_DB}' AS ref (READ_ONLY)")
    con.execute("CREATE VIEW dvf AS SELECT * FROM ref.dvf")
    yield con
    con.close()


def test_commune_agg_type(con_ref) -> None:
    commune_agg_type(con_ref, "dvf")

    n = con_ref.execute("SELECT count(*) FROM commune_agg_type").fetchone()[0]
    assert n == REF_COMMUNE_TYPE_ROWS

    types = {
        row[0]
        for row in con_ref.execute("SELECT DISTINCT type_local FROM commune_agg_type").fetchall()
    }
    assert types == {"Maison", "Appartement"}

    # Cohérence : la somme des transactions par type = total de la table dvf.
    total = con_ref.execute("SELECT sum(nb_transactions) FROM commune_agg_type").fetchone()[0]
    ref_total = con_ref.execute("SELECT count(*) FROM dvf").fetchone()[0]
    assert total == ref_total


def test_dept_agg(con_ref) -> None:
    dept_agg(con_ref, "dvf")

    n_tous_types = con_ref.execute(
        "SELECT count(*) FROM dept_agg WHERE type_local IS NULL"
    ).fetchone()[0]
    assert n_tous_types == REF_DEPTS

    n_par_type = con_ref.execute(
        "SELECT count(*) FROM dept_agg WHERE type_local IS NOT NULL"
    ).fetchone()[0]
    assert n_par_type == REF_DEPTS * 2  # Maison + Appartement partout

    # La ligne tous-types d'un département = somme de ses lignes par type.
    incoherents = con_ref.execute(
        """
        SELECT count(*) FROM (
            SELECT t.code_departement
            FROM dept_agg t
            JOIN (SELECT code_departement, sum(nb_transactions) AS s
                  FROM dept_agg WHERE type_local IS NOT NULL GROUP BY 1) p
              USING (code_departement)
            WHERE t.type_local IS NULL AND t.nb_transactions <> p.s
        )
        """
    ).fetchone()[0]
    assert incoherents == 0
