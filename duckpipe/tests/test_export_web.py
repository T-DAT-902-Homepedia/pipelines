"""Tests hors-ligne des builders d'export web sur fixtures synthétiques."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from duckpipe import export_web
from duckpipe.connection import get_connection


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

    iles = fiches["2A004"]  # jamais 404 : fiche présente, score/prix null
    assert iles["prix"] is None
    assert iles["score"] is None
    assert iles["indicateurs"]["surface_km2"] == 8.0

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
