"""Tests hors-ligne des builders d'export web sur fixtures synthétiques."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from duckpipe import export_web
from duckpipe.connection import get_connection
from duckpipe.pipelines.codes import dept_code_expr


@pytest.fixture
def geo_con():
    con = get_connection()  # extension spatial chargée
    # Trois communes : deux carrés adjacents (frontière partagée) + une
    # commune à deux îles (MultiPolygon) pour tester le regroupement ST_Dump.
    con.execute(
        """
        CREATE TABLE commune_geom AS
        SELECT * FROM (VALUES
            ('01001', 'Alpha', ST_GeomFromText(
                'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), 10.0),
            ('01002', 'Beta', ST_GeomFromText(
                'POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))'), 12.0),
            ('2A004', 'Iles', ST_GeomFromText(
                'MULTIPOLYGON(((3 0, 4 0, 4 1, 3 1, 3 0)), ((5 0, 6 0, 6 1, 5 1, 5 0)))'), 8.0)
        ) AS v(code_commune, nom_commune, geom, surface_km2)
        """
    )
    yield con
    con.close()


def test_dept_expr_handles_corse_and_outre_mer(geo_con) -> None:
    geo_con.execute(
        "CREATE TABLE codes AS SELECT unnest(['01001', '2A004', '2B033', '97411']) AS code_commune"
    )
    rows = geo_con.execute(
        f"SELECT code_commune, {export_web.DEPT_EXPR} FROM codes ORDER BY 1"
    ).fetchall()
    assert dict(rows) == {"01001": "01", "2A004": "2A", "2B033": "2B", "97411": "974"}


@pytest.fixture
def full_con(geo_con):
    con = geo_con
    con.execute(
        "CREATE TABLE commune_agg AS SELECT * FROM (VALUES "
        "('01001', 'Alpha', '01', 10, 2000.0, 1800.0, 2200.0, 2010.0, true), "
        "('01002', 'Beta', '01', 3, 1500.0, 1400.0, 1600.0, 1510.0, false)"
        ") AS v(code_commune, nom_commune, code_departement, nb_transactions, "
        "prix_m2_median, prix_m2_p25, prix_m2_p75, prix_m2_moyen, fiable)"
    )
    con.execute(
        "CREATE TABLE commune_agg_type AS SELECT * FROM (VALUES "
        "('01001', 'Maison', 'Alpha', '01', 6, 2100.0, true), "
        "('01001', 'Appartement', 'Alpha', '01', 4, 1900.0, false)"
        ") AS v(code_commune, type_local, nom_commune, code_departement, "
        "nb_transactions, prix_m2_median, fiable)"
    )
    # '99999' : commune scorée absente des contours (cas des communes DVF
    # fusionnées entre millésimes) — doit quand même avoir fiche + entrée index.
    con.execute(
        "CREATE TABLE score_territoire AS "
        "SELECT '01001' AS code_commune, 'Alpha' AS nom_commune, "
        "'01' AS code_departement, 2000.0 AS prix_m2_median, "
        "0.6 AS score_valeur, 0.1 AS gap, 0.05 AS gap_pondere, 'D' AS dpe_dominant, "
        "0.4 AS n_prix, 0.2 AS n_transport, 0.5 AS n_access_fin, 0.9 AS n_risques, "
        "0.1 AS n_tourisme, 0.8 AS n_securite, 0.3 AS n_services, 0.2 AS n_loisirs, "
        "0.5 AS n_ensoleillement, 0.6 AS n_emploi, 0.7 AS n_proximite, 0.5 AS n_dpe "
        "UNION ALL SELECT '99999', 'Fantome', '99', 1000.0, "
        "0.3, 0.05, 0.02, 'E', 0.2, 0.1, 0.3, 0.5, 0.1, 0.4, 0.2, 0.1, 0.3, 0.4, 0.5, 0.3"
    )
    return con


def test_choropleth_communes_properties(full_con, tmp_path: Path) -> None:
    export_web.build_choropleth_communes(
        full_con, "commune_geom", "commune_agg", "commune_agg_type", "score_territoire"
    )
    dest = tmp_path / "communes.geojson"
    full_con.execute(
        f"COPY (SELECT * FROM web_choropleth_communes) TO '{dest}' "
        f"(FORMAT GDAL, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'COORDINATE_PRECISION=4')"
    )

    collection = json.loads(dest.read_text())
    assert collection["type"] == "FeatureCollection"
    features = {f["properties"]["code_commune"]: f["properties"] for f in collection["features"]}
    assert len(features) == 3

    alpha = features["01001"]
    assert alpha["prix_m2_median"] == 2000.0
    assert alpha["maison_prix_m2_median"] == 2100.0
    assert alpha["appart_fiable"] is False
    assert alpha["score_valeur"] == 0.6
    assert alpha["gap"] == 0.1
    assert alpha["dpe_dominant"] == "D"
    assert alpha["n_emploi"] == 0.6

    iles = features["2A004"]  # commune sans transaction ni score
    assert iles["prix_m2_median"] is None
    assert iles["nb_transactions"] == 0
    assert iles["fiable"] is False
    assert iles["score_valeur"] is None
    assert iles["dpe_dominant"] is None
    assert iles["n_prix"] is None
    assert iles["code_departement"] == "2A"


def test_score_compat_contract(full_con, tmp_path: Path) -> None:
    """Contrat hérité de webapp_export/export_score_geojson.py (ADR-0014) :
    communes scorées avec géométrie uniquement, noms de properties exacts."""
    choropleth = export_web.build_choropleth_communes(
        full_con, "commune_geom", "commune_agg", "commune_agg_type", "score_territoire"
    )
    export_web.build_score_geojson_compat(full_con, choropleth)
    dest = tmp_path / "score.geojson"
    full_con.execute(
        f"COPY (SELECT * FROM web_score_compat) TO '{dest}' "
        f"(FORMAT GDAL, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'COORDINATE_PRECISION=3')"
    )

    features = json.loads(dest.read_text())["features"]
    # 01001 seule : 01002/2A004 non scorées, 99999 scorée mais sans contour.
    assert [f["properties"]["code_commune"] for f in features] == ["01001"]

    alpha = features[0]["properties"]
    expected_keys = {
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
    assert set(alpha) == expected_keys
    assert alpha["nom"] == "Alpha"
    assert alpha["dep"] == "01"
    assert alpha["prix"] == 2000
    assert alpha["dpe"] == "D"
    assert alpha["gap"] == 0.1
    assert alpha["n_emploi"] == 0.6


def _seed_avis(con) -> None:
    """Table gold ``avis_commune`` synthétique : Alpha (riche), Beta (low_data)."""
    con.execute(
        """
        CREATE TABLE avis_commune AS
        SELECT
            '01001' AS code_commune, 'Alpha' AS nom_ville, 12 AS n_avis,
            DATE '2019-05-01' AS date_min, DATE '2025-01-01' AS date_max,
            0.42 AS sentiment_global, false AS low_data,
            [{'theme': 'calme', 'n_segments': 8, 'pct_positive': 0.7,
              'pct_negative': 0.2, 'score': 0.5}] AS themes,
            [{'word': 'calme', 'weight': 9, 'sentiment': 'positive',
              'themes': ['calme']}] AS wordcloud,
            [{'word': 'calme', 'weight': 9, 'sentiment': 'positive'}] AS wordcloud_preview,
            [{'text': 'Quartier très calme et familial, idéal pour les enfants.',
              'label': 'Positif', 'theme': 'calme', 'mois': '2024-03',
              'source': 'Ville-idéale'}] AS verbatims
        UNION ALL
        SELECT
            '01002', 'Beta', 4, DATE '2022-01-01', DATE '2023-01-01',
            -0.1, true,
            [{'theme': 'transports', 'n_segments': 3, 'pct_positive': 0.3,
              'pct_negative': 0.6, 'score': -0.3}],
            [{'word': 'bruit', 'weight': 3, 'sentiment': 'negative',
              'themes': ['calme']}],
            [{'word': 'bruit', 'weight': 3, 'sentiment': 'negative'}],
            [{'text': 'Stationnement compliqué le soir, il faut tourner longtemps.',
              'label': 'Négatif', 'theme': 'transports', 'mois': '2022-06',
              'source': 'Ville-idéale'}]
        """
    )


def test_build_avis_masks_themes_when_low_data(full_con) -> None:
    _seed_avis(full_con)
    export_web.build_avis(full_con, "avis_commune")
    rows = {
        r[0]: r
        for r in full_con.execute(
            "SELECT code_commune, code_departement, low_data, themes, sentiment_global, source "
            "FROM web_avis"
        ).fetchall()
    }
    alpha = rows["01001"]
    assert alpha[1] == "01"  # code_departement dérivé
    assert alpha[2] is False
    assert alpha[3] is not None  # thèmes conservés (riche)
    assert alpha[4] == 0.42
    assert alpha[5] == "Ville-idéale"

    beta = rows["01002"]
    assert beta[2] is True
    assert beta[3] is None  # thèmes masqués (low_data)


def test_avis_stub_is_empty_and_typed(full_con) -> None:
    export_web.create_avis_stub(full_con)
    n = full_con.execute("SELECT count(*) FROM avis_commune").fetchone()[0]
    assert n == 0
    # build_avis sur le stub produit une table vide sans erreur de type
    export_web.build_avis(full_con, "avis_commune")
    assert full_con.execute("SELECT count(*) FROM web_avis").fetchone()[0] == 0


def test_fiches_build_with_avis_stub(full_con) -> None:
    # Fiches doivent se construire même sans avis (avis = null partout).
    export_web.build_evolution(full_con, "commune_agg", 2024, {})
    for name, cols in [
        ("revenus", "24000.0 AS revenu_median"),
        ("emploi", "6.0 AS taux_chomage, 0.9 AS taux_couverture_emploi, 500 AS pop_active"),
        ("commune_transport", "1.5 AS densite_arrets_km2, 12 AS nb_arrets"),
        ("equipements", "3 AS nb_services_sante, 2 AS nb_loisirs_culture"),
        ("securite", "30.0 AS taux_delinquance_global, 800 AS insee_pop"),
        ("tourisme", "0.05 AS part_residences_secondaires"),
        ("risques", "2 AS nb_arretes_catnat"),
        ("climat", "1900.0 AS ensoleillement_h_an, 12.0 AS temperature_moy_annuelle"),
        ("proximite_metropole", "30.0 AS dist_metropole_km, 'Lyon' AS nom_metropole"),
    ]:
        full_con.execute(f"CREATE TABLE {name} AS SELECT '01001' AS code_commune, {cols}")
    export_web.create_avis_stub(full_con)
    export_web.build_fiches(
        full_con, "commune_geom", "commune_agg", "commune_agg_type", "score_territoire",
        "web_evolution", "revenus", "emploi", "commune_transport", "equipements",
        "securite", "tourisme", "risques", "climat", "proximite_metropole", "avis_commune",
    )
    avis_vals = full_con.execute("SELECT avis FROM web_fiches").fetchall()
    assert all(v[0] is None for v in avis_vals)


def test_fiches_structure(full_con, tmp_path: Path) -> None:
    full_con.execute("CREATE TABLE commune_prix_2021 AS SELECT '01001' AS code_commune, "
                     "38 AS nb_transactions, 1900.0 AS prix_m2_median")
    export_web.build_evolution(full_con, "commune_agg", 2024, {2021: "commune_prix_2021"})
    for name, cols in [
        ("revenus", "24000.0 AS revenu_median"),
        ("emploi", "6.0 AS taux_chomage, 0.9 AS taux_couverture_emploi, 500 AS pop_active"),
        ("commune_transport", "1.5 AS densite_arrets_km2, 12 AS nb_arrets"),
        ("equipements", "3 AS nb_services_sante, 2 AS nb_loisirs_culture"),
        ("securite", "30.0 AS taux_delinquance_global, 800 AS insee_pop"),
        ("tourisme", "0.05 AS part_residences_secondaires"),
        ("risques", "2 AS nb_arretes_catnat"),
        ("climat", "1900.0 AS ensoleillement_h_an, 12.0 AS temperature_moy_annuelle"),
        ("proximite_metropole", "30.0 AS dist_metropole_km, 'Lyon' AS nom_metropole"),
    ]:
        full_con.execute(f"CREATE TABLE {name} AS SELECT '01001' AS code_commune, {cols}")

    _seed_avis(full_con)
    export_web.build_fiches(
        full_con,
        "commune_geom",
        "commune_agg",
        "commune_agg_type",
        "score_territoire",
        "web_evolution",
        *[
            "revenus",
            "emploi",
            "commune_transport",
            "equipements",
            "securite",
            "tourisme",
            "risques",
            "climat",
            "proximite_metropole",
        ],
        "avis_commune",
    )
    dest = tmp_path / "fiches.json"
    full_con.execute(f"COPY (SELECT * FROM web_fiches ORDER BY code_commune) TO '{dest}' "
                     f"(FORMAT JSON, ARRAY true)")
    fiches = {f["code_commune"]: f for f in json.loads(dest.read_text())}

    alpha = fiches["01001"]
    assert alpha["prix"]["median"] == 2000.0
    assert alpha["prix"]["maison"]["median"] == 2100.0
    assert alpha["score"]["composantes"]["n_emploi"] == 0.6
    assert alpha["indicateurs"]["nom_metropole"] == "Lyon"
    assert [entry["annee"] for entry in alpha["evolution"]] == [2021, 2024]
    # aperçu avis embarqué dans la fiche
    assert alpha["avis"]["n_avis"] == 12
    assert alpha["avis"]["low_data"] is False
    assert len(alpha["avis"]["mini_cloud"]) == 1

    iles = fiches["2A004"]  # jamais 404 : fiche présente, score/prix null
    assert iles["prix"] is None
    assert iles["score"] is None
    assert iles["indicateurs"]["surface_km2"] == 8.0
    assert iles["avis"] is None  # pas d'avis pour cette commune

    fantome = fiches["99999"]  # scorée mais absente des contours : fiche quand même
    assert fantome["score"]["score_valeur"] == 0.3
    assert fantome["indicateurs"]["surface_km2"] is None


def test_search_index_and_classement(full_con, tmp_path: Path) -> None:
    export_web.build_search_index(
        full_con, "commune_geom", "commune_agg", "score_territoire"
    )
    index_dest = tmp_path / "index.json"
    full_con.execute(
        f"COPY (SELECT * FROM web_search_index) TO '{index_dest}' (FORMAT JSON, ARRAY true)"
    )
    index = json.loads(index_dest.read_text())
    assert len(index) == 4  # 3 contours + 1 scorée hors contours
    assert index[0] == {"c": "01001", "n": "Alpha", "d": "01", "p": 2000, "s": 0.6}
    assert index[3]["c"] == "99999"

    export_web.build_classement(full_con, "score_territoire")
    top = full_con.execute("SELECT rang, code_commune FROM web_classement").fetchall()
    assert top == [(1, "01001"), (2, "99999")]


@pytest.fixture
def dvf_con(full_con):
    """Transactions synthétiques : Alpha (01, ARA) 5 maisons + 4 apparts,
    Gamma (2A004, Corse) 2 maisons. Prix choisis pour distinguer une médiane
    de transactions d'une médiane de médianes communales."""
    con = full_con
    con.execute(
        """
        CREATE TABLE dvf AS
        SELECT * FROM (VALUES
            ('01001', '01', 'Maison', 1000.0, 4.001234567, 46.2),
            ('01001', '01', 'Maison', 2000.0, 4.0, 46.2),
            ('01001', '01', 'Maison', 3000.0, 4.0, 46.2),
            ('01001', '01', 'Maison', 4000.0, 4.0, 46.2),
            ('01001', '01', 'Maison', 5000.0, 4.0, 46.2),
            ('01001', '01', 'Appartement', 1500.0, 4.0, 46.2),
            ('01001', '01', 'Appartement', 2500.0, 4.0, 46.2),
            ('01001', '01', 'Appartement', 3500.0, 4.0, 46.2),
            ('01001', '01', 'Appartement', 4500.0, 4.0, 46.2),
            ('2A004', '2A', 'Maison', 6000.0, 8.7, 41.9),
            ('2A004', '2A', 'Maison', 8000.0, 8.7, 41.9)
        ) AS v(code_commune, code_departement, type_local, prix_m2, longitude, latitude)
        """
    )
    con.execute(
        """
        CREATE TABLE region_geom_1000m AS
        SELECT * FROM (VALUES
            ('84', 'Auvergne-Rhône-Alpes', ST_GeomFromText(
                'POLYGON((0 0, 2 0, 2 1, 0 1, 0 0))')),
            ('94', 'Corse', ST_GeomFromText(
                'POLYGON((3 0, 6 0, 6 1, 3 1, 3 0))')),
            ('11', 'Île-de-France', ST_GeomFromText(
                'POLYGON((10 10, 11 10, 11 11, 10 11, 10 10))'))
        ) AS v(code_region, nom_region, geom)
        """
    )
    return con


def test_choropleth_regions_from_transactions(dvf_con, tmp_path: Path) -> None:
    export_web.build_choropleth_regions(
        dvf_con, "region_geom_1000m", "dvf", "score_territoire"
    )
    dest = tmp_path / "regions.geojson"
    dvf_con.execute(
        f"COPY (SELECT * FROM web_choropleth_regions) TO '{dest}' "
        f"(FORMAT GDAL, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'COORDINATE_PRECISION=2')"
    )
    features = {
        f["properties"]["code_region"]: f["properties"]
        for f in json.loads(dest.read_text())["features"]
    }
    assert len(features) == 3

    ara = features["84"]
    assert ara["nom"] == "Auvergne-Rhône-Alpes"
    # Médiane des 9 TRANSACTIONS d'Alpha (3000), pas des médianes communales.
    assert ara["prix_m2_median"] == 3000.0
    assert ara["nb_transactions"] == 9
    assert ara["fiable"] is True
    assert ara["maison_prix_m2_median"] == 3000.0
    assert ara["maison_nb_transactions"] == 5
    assert ara["appart_prix_m2_median"] == 3000.0
    assert ara["appart_nb_transactions"] == 4
    # Score : Alpha est la seule commune scorée de la région (99999 -> dept 99
    # hors mapping, exclue).
    assert ara["nb_communes_scorees"] == 1
    assert ara["score_median"] == 0.6
    assert ara["gap_pondere_median"] == 0.05

    corse = features["94"]
    assert corse["nb_transactions"] == 2
    assert corse["fiable"] is False
    assert corse["prix_m2_median"] == 7000.0
    assert corse["appart_prix_m2_median"] is None

    idf = features["11"]  # région sans donnée : properties nulles, pas d'erreur
    assert idf["prix_m2_median"] is None
    assert idf["nb_transactions"] == 0
    assert idf["nb_communes_scorees"] == 0


def test_choropleth_departements_expose_region(full_con, tmp_path: Path) -> None:
    full_con.execute(
        """
        CREATE TABLE dept_geom_1000m AS
        SELECT '01' AS code_departement, 'Ain' AS nom_departement,
               ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS geom
        """
    )
    full_con.execute(
        "CREATE TABLE dept_agg AS SELECT * FROM (VALUES "
        "('01', NULL, 13, 2200.0, true), ('01', 'Maison', 8, 2400.0, true)"
        ") AS v(code_departement, type_local, nb_transactions, prix_m2_median, fiable)"
    )
    export_web.build_choropleth_departements(full_con, "dept_geom_1000m", "dept_agg")
    row = full_con.execute(
        "SELECT code_region, nom_region FROM web_choropleth_departements"
    ).fetchone()
    assert row == ("84", "Auvergne-Rhône-Alpes")


def test_region_mapping_covers_all_departements(con) -> None:
    export_web.ensure_region_mapping(con)
    n_depts, n_regions = con.execute(
        "SELECT count(*), count(DISTINCT code_region) FROM region_mapping"
    ).fetchone()
    assert n_depts == 101  # 96 métropole (dont 2A/2B) + 5 DROM
    assert n_regions == 18


def test_points_sample_is_deterministic_and_rounded(dvf_con) -> None:
    export_web.build_points_sample(dvf_con, "dvf", n=5)
    first = dvf_con.execute("SELECT * FROM web_points_sample ORDER BY 1, 2, 3").fetchall()
    export_web.build_points_sample(dvf_con, "dvf", n=5)
    second = dvf_con.execute("SELECT * FROM web_points_sample ORDER BY 1, 2, 3").fetchall()
    assert first == second
    assert len(first) == 5
    cols = [d[0] for d in dvf_con.description]
    assert cols == ["lon", "lat", "prix", "t"]
    lons = {r[0] for r in first}
    assert lons <= {4.0, 4.0012, 8.7}  # arrondi 4 décimales
    assert {r[3] for r in first} <= {"M", "A"}


def test_prix_distribution_envelope(dvf_con) -> None:
    payload = export_web.build_prix_distribution(dvf_con, "dvf", year=2024, bins=10)
    assert payload["schema_version"] == 1
    assert payload["year"] == 2024
    assert len(payload["bin_edges"]) == 11
    assert payload["bin_edges"][0] == 0
    # Chaque série couvre toutes les transactions <= p99 arrondi.
    assert sum(payload["series"]["maison"]) + sum(payload["series"]["appartement"]) == sum(
        payload["series"]["tous"]
    )
    assert sum(payload["series"]["tous"]) >= 10


def test_prix_series_weighted_median_and_nulls(full_con) -> None:
    # Millésime 2021 : Alpha 8 ventes à 1800, Beta 2 ventes (sous le seuil -> null).
    full_con.execute(
        "CREATE TABLE commune_prix_2021 AS SELECT * FROM (VALUES "
        "('01001', 8, 1800.0), ('01002', 2, 1400.0)"
        ") AS v(code_commune, nb_transactions, prix_m2_median)"
    )
    payload = export_web.build_prix_series(
        full_con, "commune_agg", 2024, {2021: "commune_prix_2021"}
    )
    assert payload["years"] == [2021, 2024]
    # Médiane pondérée 2021 : 10 ventes, la 5e est à 1800 (8 ventes Alpha).
    assert payload["national"] == [1800, 2000]
    assert payload["communes"]["01001"] == [1800, 2000]
    # Beta : sous le seuil en 2021, 3 ventes < 5 en 2024 -> null partout.
    assert payload["communes"].get("01002", [None, None]) == [None, None]


def test_dept_code_expr_pads_without_truncating(con) -> None:
    rows = con.execute(
        f"SELECT code, {dept_code_expr('code')} FROM (VALUES "
        "('1'), ('01'), ('2A'), ('971'), (' 974 ')) AS v(code)"
    ).fetchall()
    assert dict(rows) == {
        "1": "01",
        "01": "01",
        "2A": "2A",
        "971": "971",  # lpad natif tronquerait en '97'
        " 974 ": "974",
    }
