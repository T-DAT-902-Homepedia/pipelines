"""Tests hors-ligne de l'écriture GeoJSON web (publish_web._copy_geojson).

L'arrondi GDAL COORDINATE_PRECISION effondre les îles plus petites que la
grille en Point/LineString et emballe alors la feature dans une
GeometryCollection (cas réel : Nouvelle-Aquitaine invisible sur la carte,
collection imbriquée). La réparation post-écriture doit garantir un contrat
Polygon/MultiPolygon sur tous les artefacts.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from duckpipe.connection import get_connection
from duckpipe.publish_web import _copy_geojson, _repair_geometry_collections


@pytest.fixture
def con():
    con = get_connection()
    yield con
    con.close()


def _feature_collection(geometry: dict) -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"code": "75"}, "geometry": geometry}
        ],
    }


def test_copy_geojson_repare_les_iles_effondrees(con, tmp_path: Path) -> None:
    """Grand carré + île sous la grille : sans réparation, GDAL produit une
    GeometryCollection[Polygon, Point] à COORDINATE_PRECISION=2."""
    con.execute(
        "CREATE TABLE t AS SELECT 'X' AS code, ST_GeomFromText("
        "'MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), "
        "((2.001 2.001, 2.003 2.001, 2.002 2.003, 2.001 2.001)))') AS geom"
    )
    dest = tmp_path / "out.geojson"
    _copy_geojson(con, "SELECT * FROM t", dest, precision=2)

    data = json.loads(dest.read_text())
    types = {f["geometry"]["type"] for f in data["features"]}
    assert types <= {"Polygon", "MultiPolygon"}
    # Le carré principal survit (l'île effondrée est écartée).
    (feature,) = data["features"]
    assert [0.0, 0.0] in feature["geometry"]["coordinates"][0]


def test_repare_collection_imbriquee_nouvelle_aquitaine(tmp_path: Path) -> None:
    """Cas réel regions-low : GC[GC[MultiPolygon, LineString], MultiLineString]."""
    ring1 = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]
    ring2 = [[3, 3], [4, 3], [4, 4], [3, 4], [3, 3]]
    geometry = {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "GeometryCollection",
                "geometries": [
                    {"type": "MultiPolygon", "coordinates": [[ring1], [ring2]]},
                    {"type": "LineString", "coordinates": [[5, 5], [5, 6]]},
                ],
            },
            {"type": "MultiLineString", "coordinates": [[[6, 6], [6, 7]]]},
        ],
    }
    dest = tmp_path / "regions.geojson"
    dest.write_text(json.dumps(_feature_collection(geometry)))

    _repair_geometry_collections(dest)

    (feature,) = json.loads(dest.read_text())["features"]
    assert feature["geometry"] == {
        "type": "MultiPolygon",
        "coordinates": [[ring1], [ring2]],
    }


def test_repare_collection_simple_en_polygon(tmp_path: Path) -> None:
    """Une seule partie polygonale restante -> Polygon, pas MultiPolygon."""
    ring = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]
    geometry = {
        "type": "GeometryCollection",
        "geometries": [
            {"type": "Polygon", "coordinates": [ring]},
            {"type": "Point", "coordinates": [2, 2]},
        ],
    }
    dest = tmp_path / "depts.geojson"
    dest.write_text(json.dumps(_feature_collection(geometry)))

    _repair_geometry_collections(dest)

    (feature,) = json.loads(dest.read_text())["features"]
    assert feature["geometry"] == {"type": "Polygon", "coordinates": [ring]}


def test_fichier_sans_collection_non_reecrit(tmp_path: Path) -> None:
    ring = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]
    dest = tmp_path / "communes.geojson"
    original = json.dumps(
        _feature_collection({"type": "Polygon", "coordinates": [ring]})
    )
    dest.write_text(original)

    _repair_geometry_collections(dest)

    assert dest.read_text() == original


def test_collection_sans_polygone_devient_null(tmp_path: Path) -> None:
    geometry = {
        "type": "GeometryCollection",
        "geometries": [{"type": "Point", "coordinates": [2, 2]}],
    }
    dest = tmp_path / "degenere.geojson"
    dest.write_text(json.dumps(_feature_collection(geometry)))

    _repair_geometry_collections(dest)

    (feature,) = json.loads(dest.read_text())["features"]
    assert feature["geometry"] is None
