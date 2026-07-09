"""Tests hors-ligne de la maille quartier (IRIS) sur fixtures synthétiques :
contours silver, affectation spatiale des mutations et gold score_quartier.
"""

from __future__ import annotations

import pytest

from duckpipe.connection import get_connection
from duckpipe.pipelines.iris import iris_geom, iris_prix
from duckpipe.pipelines.score_quartier import score_quartier


@pytest.fixture
def geo_con():
    con = get_connection()  # extension spatial chargée
    yield con
    con.close()


def _seed_iris_geom(con) -> None:
    """Référentiel synthétique : Alpha mono-IRIS, Beta = deux carrés IRIS
    adjacents (frontière partagée x=1.5), plus un arrondissement PLM."""
    con.execute(
        """
        CREATE TABLE iris_geom AS SELECT * FROM (VALUES
            ('010010000', '01001', 'Alpha', 'Alpha', 'Z', 1,
             ST_GeomFromText('POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))')),
            ('010020101', '01002', 'Beta', 'Beta Ouest', 'H', 2,
             ST_GeomFromText('POLYGON((1 0, 1.5 0, 1.5 1, 1 1, 1 0))')),
            ('010020102', '01002', 'Beta', 'Beta Est', 'H', 2,
             ST_GeomFromText('POLYGON((1.5 0, 2 0, 2 1, 1.5 1, 1.5 0))')),
            ('751080301', '75108', 'Paris 8e Arrondissement', 'Europe', 'H', 3,
             ST_GeomFromText('POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))'))
        ) AS v(code_iris, code_commune, nom_commune, nom_iris, type_iris,
               nb_iris_commune, geom)
        """
    )


def test_iris_geom_counts_and_pads(geo_con) -> None:
    """Node iris_geom : lpad du code commune, nb_iris_commune par fenêtre,
    codes arrondissement PLM conservés tels quels."""
    geo_con.execute(
        """
        CREATE TABLE iris_raw AS SELECT * FROM (VALUES
            ('1001', 'Alpha', '010010000', 'Alpha', 'Z',
             ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')),
            ('01002', 'Beta', '010020101', 'Beta Ouest', 'H',
             ST_GeomFromText('POLYGON((1 0, 1.5 0, 1.5 1, 1 1, 1 0))')),
            ('01002', 'Beta', '010020102', 'Beta Est', 'H',
             ST_GeomFromText('POLYGON((1.5 0, 2 0, 2 1, 1.5 1, 1.5 0))')),
            ('75108', 'Paris 8e Arrondissement', '751080301', 'Europe', 'H',
             ST_GeomFromText('POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))')),
            ('99999', 'Degenere', NULL, 'Sans code', 'Z',
             ST_GeomFromText('POLYGON((7 7, 8 7, 8 8, 7 8, 7 7))'))
        ) AS v(code_insee, nom_commune, code_iris, nom_iris, type_iris, geom)
        """
    )
    iris_geom(geo_con, "iris_raw")

    rows = {
        r[0]: r
        for r in geo_con.execute(
            "SELECT code_iris, code_commune, nb_iris_commune FROM iris_geom"
        ).fetchall()
    }
    assert len(rows) == 4  # la ligne sans code_iris est écartée
    assert rows["010010000"][1] == "01001"  # lpad du '1001' source
    assert rows["010010000"][2] == 1
    assert rows["010020101"][2] == 2
    assert rows["010020102"][2] == 2
    # PLM : l'arrondissement est conservé (jamais aligné sur commune_geom).
    assert rows["751080301"][1] == "75108"


@pytest.fixture
def prix_con(geo_con):
    """Mutations synthétiques posées sur le référentiel _seed_iris_geom :
    Alpha 5 ventes (hors géométrie : l'affectation mono-IRIS est un equi-join),
    Beta 2 ventes à l'ouest, 1 à l'est, 1 pile sur la frontière, plus une
    commune orpheline absente du référentiel."""
    con = geo_con
    _seed_iris_geom(con)
    con.execute(
        """
        CREATE TABLE dvf AS SELECT * FROM (VALUES
            ('2024-1', '01001', 0.0, 0.0, 1000.0),
            ('2024-2', '01001', 0.0, 0.0, 1100.0),
            ('2024-3', '01001', 0.0, 0.0, 1200.0),
            ('2024-4', '01002', 1.2, 0.5, 2000.0),
            ('2024-5', '01002', 1.8, 0.5, 4000.0),
            ('2024-6', '01002', 1.5, 0.5, 3000.0),
            ('2024-7', '99999', 0.5, 0.5, 9000.0)
        ) AS v(id_mutation, code_commune, longitude, latitude, prix_m2)
        """
    )
    con.execute(
        """
        CREATE TABLE dvf_points_2021 AS SELECT * FROM (VALUES
            ('2021-1', '01001', 0.0, 0.0, 900.0, 'Maison'),
            ('2021-2', '01001', 0.0, 0.0, 950.0, 'Maison'),
            ('2021-3', '01002', 1.3, 0.2, 1800.0, 'Appartement')
        ) AS v(id_mutation, code_commune, longitude, latitude, prix_m2, type_local)
        """
    )
    return con


def test_iris_prix_affectation_et_pool(prix_con) -> None:
    iris_prix(
        prix_con, "iris_geom", "dvf", year=2024, points_tables={2021: "dvf_points_2021"}
    )
    rows = {
        r[0]: r
        for r in prix_con.execute(
            "SELECT code_iris, code_commune, nb_transactions, prix_m2_median, fiable, "
            "annee_min, annee_max, nb_millesimes FROM iris_prix"
        ).fetchall()
    }

    # Alpha mono-IRIS : 3 ventes 2024 + 2 ventes 2021 affectées SANS test
    # géométrique (les points sont volontairement hors du polygone) -> fiable.
    alpha = rows["010010000"]
    assert alpha[2] == 5
    assert alpha[4] is True
    assert alpha[3] == 1000.0  # médiane de 900/950/1000/1100/1200
    assert (alpha[5], alpha[6], alpha[7]) == (2021, 2024, 2)

    # Beta Ouest : 2 ventes dedans + le point frontière (1.5, 0.5) départagé
    # au plus petit code_iris.
    ouest = rows["010020101"]
    assert ouest[2] == 3
    assert ouest[4] is False  # < 5 ventes

    est = rows["010020102"]
    assert est[2] == 1

    # La commune orpheline (hors référentiel IRIS) est perdue, pas réaffectée.
    assert "99999" not in {r[1] for r in rows.values()}
    total = sum(r[2] for r in rows.values())
    assert total == 9  # 10 mutations poolées - 1 orpheline


def test_iris_prix_est_deterministe(prix_con) -> None:
    iris_prix(
        prix_con, "iris_geom", "dvf", year=2024, points_tables={2021: "dvf_points_2021"}
    )
    first = prix_con.execute("SELECT * FROM iris_prix ORDER BY code_iris").fetchall()
    iris_prix(
        prix_con, "iris_geom", "dvf", year=2024, points_tables={2021: "dvf_points_2021"}
    )
    second = prix_con.execute("SELECT * FROM iris_prix ORDER BY code_iris").fetchall()
    assert first == second


def test_score_quartier_gap_et_exclusions(geo_con) -> None:
    """Gold : n_prix_iris normalisé sur les IRIS fiables, gap = score commune
    hérité - n_prix_iris, exclusion des non-fiables et des communes non
    scorées, raccord PLM par code arrondissement."""
    con = geo_con
    _seed_iris_geom(con)
    con.execute(
        """
        CREATE TABLE iris_prix AS SELECT * FROM (VALUES
            ('010010000', '01001', 8, 1000.0::DOUBLE, true, 2021, 2024, 2),
            ('010020101', '01002', 6, 2000.0::DOUBLE, true, 2021, 2024, 2),
            ('751080301', '75108', 12, 3000.0::DOUBLE, true, 2021, 2024, 2),
            ('010020102', '01002', 2, 9000.0::DOUBLE, false, 2024, 2024, 1),
            ('970010101', '97001', 7, 1500.0::DOUBLE, true, 2021, 2024, 2)
        ) AS v(code_iris, code_commune, nb_transactions, prix_m2_median, fiable,
               annee_min, annee_max, nb_millesimes)
        """
    )
    # 97001 volontairement absent : commune non scorée -> IRIS exclu du gold
    # SANS influencer la normalisation (population = gold uniquement).
    con.execute(
        """
        CREATE TABLE score_territoire AS SELECT * FROM (VALUES
            ('01001', 'Alpha', '01', 0.6::DOUBLE, 0.4::DOUBLE, 0.5::DOUBLE),
            ('01002', 'Beta', '01', 0.3::DOUBLE, 0.2::DOUBLE, 0.8::DOUBLE),
            ('75108', 'Paris 8e Arrondissement', '75',
             0.9::DOUBLE, 0.95::DOUBLE, 0.1::DOUBLE)
        ) AS v(code_commune, nom_commune, code_departement, score_valeur,
               n_prix, n_access_fin)
        """
    )

    score_quartier(con, "iris_prix", "iris_geom", "score_territoire")
    rows = {
        r[0]: r
        for r in con.execute(
            "SELECT code_iris, code_commune, n_prix_iris, score_commune, gap_iris, "
            "gap_pondere_iris, nom_iris, code_departement FROM score_quartier"
        ).fetchall()
    }

    # Non-fiable et commune non scorée exclus.
    assert set(rows) == {"010010000", "010020101", "751080301"}

    # _norm sur 3 prix fiables retenus (1000/2000/3000) : clip p1-p99 puis
    # minmax -> 0, 0.5, 1 exactement.
    assert rows["010010000"][2] == pytest.approx(0.0)
    assert rows["010020101"][2] == pytest.approx(0.5)
    assert rows["751080301"][2] == pytest.approx(1.0)

    # gap_iris = score commune hérité - n_prix_iris ; gap_pondere x n_access_fin.
    assert rows["010010000"][4] == pytest.approx(0.6 - 0.0)
    assert rows["010020101"][4] == pytest.approx(0.3 - 0.5)
    assert rows["010020101"][5] == pytest.approx((0.3 - 0.5) * 0.8)

    # Raccord PLM : l'IRIS de l'arrondissement hérite du score 75108.
    paris = rows["751080301"]
    assert paris[3] == pytest.approx(0.9)
    assert paris[6] == "Europe"
    assert paris[7] == "75"

    # Bornes du MVP : n_prix_iris dans [0, 1], gap_iris dans [-1, 1].
    bounds = con.execute(
        "SELECT count(*) FROM score_quartier "
        "WHERE n_prix_iris < 0 OR n_prix_iris > 1 OR gap_iris < -1 OR gap_iris > 1"
    ).fetchone()[0]
    assert bounds == 0
