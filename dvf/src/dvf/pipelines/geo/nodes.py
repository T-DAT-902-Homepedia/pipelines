"""
Nodes du pipeline geo : contours administratifs -> PostGIS.

Hors-Spark (Spark n'a ni connecteur PostGIS ni support géométrie).

On télécharge les contours pré-simplifiés Etalab 2025 (3 niveaux de détail) et on les charge
dans des tables référentiel PostGIS (pré-créées par db-init, cf. db/schemas/),
une colonne géométrie par niveau (LOD).

Ces contours ne dépendent ni du type de bien ni des prix. Ils sont ensuite joints
aux agrégats par le pipeline `choropleth` (jointure précalculée servie à l'API),
non plus au moment de la requête.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple

import httpx
import psycopg
from psycopg import sql


class _TableSpec(NamedTuple):
    """Décrit une table référentiel à charger."""

    table: str
    pk_column: str
    columns: list[str]
    feature_row: Callable[[dict], tuple]

# Niveaux de détail (LOD) -> colonne géométrie cible. L'ordre est significatif :
# "low" en premier, c'est lui qui fournit les attributs (cf. _load_table).
LOD_COLUMNS = {"low": "geom_low", "mid": "geom_mid", "high": "geom_high"}

# Le schéma (tables + index) est déclaré dans db/schemas/ et appliqué par db-init
# avant la pipeline (cf. db/README.md). Ces nodes supposent les tables existantes
# et ne font que les remplir (TRUNCATE + INSERT).


def _dsn(pg: dict) -> str:
    """Construit un DSN libpq depuis les credentials PostGIS (host côté hôte)."""
    return (
        f"host={pg.get('host', 'localhost')} port={pg.get('port', 5432)} "
        f"dbname={pg['dbname']} user={pg['user']} password={pg['password']}"
    )


def _fetch_features(url: str) -> list[dict]:
    """Télécharge un GeoJSON Etalab et renvoie ses features."""
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.json()["features"]


def _load_table(
    conn: psycopg.Connection,
    spec: _TableSpec,
    level_urls: dict[str, str],
) -> int:
    """Charge les 3 LOD dans la table (pré-existante). Renvoie le nb d'entités.

    La table et ses index sont déclarés dans db/schemas/ (appliqués par db-init) :
    ce node ne fait que la remplir (TRUNCATE + INSERT).

    ``spec.feature_row`` mappe un feature GeoJSON -> tuple des champs non-
    géométriques (hors géométrie), dans l'ordre de ``spec.columns``.
    ``spec.pk_column`` est la clé de conflit (et la 1re colonne de columns).

    Les attributs proviennent du niveau "low" : on le traite d'abord (INSERT des
    attributs + 1re géométrie), les niveaux suivants ne posent que leur géométrie
    (UPDATE). On télécharge TOUT avant de toucher la table : si un téléchargement
    échoue, la table existante reste intacte (pas de TRUNCATE prématuré).
    """
    table, columns = spec.table, spec.columns

    # 1. Téléchargements (hors transaction) : un échec réseau ne vide pas la table.
    features_by_level = {
        level: _fetch_features(url) for level, url in level_urls.items()
    }

    cols = [sql.Identifier(c) for c in columns]
    cols_csv = sql.SQL(", ").join(cols)
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(columns))
    pk = sql.Identifier(spec.pk_column)
    tbl = sql.Identifier(table)

    with conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE {};").format(tbl))

        for level, geom_col_name in LOD_COLUMNS.items():
            geom_col = sql.Identifier(geom_col_name)
            insert = sql.SQL(
                """
                INSERT INTO {tbl} ({cols}, {geom})
                VALUES ({vals},
                        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON({geom_param}), 4326)))
                ON CONFLICT ({pk}) DO UPDATE SET {geom} = EXCLUDED.{geom};
                """
            ).format(
                tbl=tbl,
                cols=cols_csv,
                geom=geom_col,
                vals=placeholders,
                geom_param=sql.Placeholder(),
                pk=pk,
            )
            for feat in features_by_level[level]:
                geom_json = psycopg.types.json.Json(feat["geometry"])
                cur.execute(insert, (*spec.feature_row(feat), geom_json))

        cur.execute(sql.SQL("SELECT count(*) FROM {};").format(tbl))
        count = cur.fetchone()[0]
    conn.commit()
    return count


def load_communes_geo(contours: dict, postgis_credentials: dict) -> int:
    """Charge les contours communes (3 LOD) dans commune_geometry."""
    spec = _TableSpec(
        table="commune_geometry",
        pk_column="code_commune",
        columns=["code_commune", "nom_commune", "code_departement"],
        feature_row=lambda f: (
            f["properties"]["code"],
            f["properties"].get("nom"),
            f["properties"].get("departement"),
        ),
    )
    with psycopg.connect(_dsn(postgis_credentials)) as conn:
        return _load_table(conn, spec, contours["communes"])


def load_departements_geo(contours: dict, postgis_credentials: dict) -> int:
    """Charge les contours départements (3 LOD) dans departement_geometry."""
    spec = _TableSpec(
        table="departement_geometry",
        pk_column="code_departement",
        columns=["code_departement", "nom"],
        feature_row=lambda f: (
            f["properties"]["code"],
            f["properties"].get("nom"),
        ),
    )
    with psycopg.connect(_dsn(postgis_credentials)) as conn:
        return _load_table(conn, spec, contours["departements"])
