"""
Nodes du pipeline choropleth : jointure précalculée géométrie × agrégats.

Hors-Spark (PostGIS/géométrie). Matérialise, par maille administrative, la
jointure entre les contours (``*_geometry``, pipeline geo) et les agrégats de
prix (``agg_*``, pipeline gold) dans une table plate ``choropleth_*`` servie
directement à l'API (plus de JOIN au moment de la requête).

Pipeline autonome : il lit les tables PostGIS déjà remplies (geo + gold) plutôt
que les datasets Kedro, pour rester lançable indépendamment (l'ordonnancement
geo+gold -> choropleth est porté par l'orchestrateur, p. ex. Airflow). Les tables
``choropleth_*`` sont pré-créées par db-init (cf. db/schemas/) : ces nodes ne
font que les remplir (TRUNCATE + INSERT ... SELECT ... JOIN).
"""

from __future__ import annotations

from typing import NamedTuple

import psycopg
from psycopg import sql


class _JoinSpec(NamedTuple):
    """Décrit une jointure choroplèthe à matérialiser."""

    target: str  # table plate produite (choropleth_*)
    geometry: str  # table géométrie source (*_geometry)
    agg: str  # table d'agrégats source (agg_*)
    join_key: str  # clé de jointure commune (code_commune / code_departement)
    geom_columns: list[str]  # attributs non-géométriques repris de la géométrie


# Colonnes d'agrégats reprises dans la table choroplèthe (celles servies à l'API).
_AGG_COLUMNS = (
    "prix_m2_median",
    "prix_m2_p25",
    "prix_m2_p75",
    "nb_transactions",
    "fiable",
)


def _dsn(pg: dict) -> str:
    """Construit un DSN libpq depuis les credentials PostGIS."""
    return (
        f"host={pg.get('host', 'localhost')} port={pg.get('port', 5432)} "
        f"dbname={pg['dbname']} user={pg['user']} password={pg['password']}"
    )


def _materialize(conn: psycopg.Connection, spec: _JoinSpec) -> int:
    """TRUNCATE + INSERT ... SELECT ... JOIN dans ``spec.target``.

    L'ordre des colonnes insérées (attributs géométrie, puis type_local, puis
    agrégats, puis les 3 géométries LOD) suit le CREATE TABLE de db/schemas/.
    """
    tbl = sql.Identifier(spec.target)
    geom_tbl = sql.Identifier(spec.geometry)
    agg_tbl = sql.Identifier(spec.agg)
    key = sql.Identifier(spec.join_key)

    geom_cols = ["geom_low", "geom_mid", "geom_high"]
    insert_cols = [
        *spec.geom_columns,
        "type_local",
        *_AGG_COLUMNS,
        *geom_cols,
    ]

    select_geom_attrs = sql.SQL(", ").join(
        sql.SQL("g.{}").format(sql.Identifier(c)) for c in spec.geom_columns
    )
    select_agg = sql.SQL(", ").join(
        sql.SQL("a.{}").format(sql.Identifier(c)) for c in _AGG_COLUMNS
    )
    select_geoms = sql.SQL(", ").join(
        sql.SQL("g.{}").format(sql.Identifier(c)) for c in geom_cols
    )

    with conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE {};").format(tbl))
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {tbl} ({cols})
                SELECT {geom_attrs}, a.type_local, {agg}, {geoms}
                FROM {geom_tbl} g
                JOIN {agg_tbl} a USING ({key});
                """
            ).format(
                tbl=tbl,
                cols=sql.SQL(", ").join(sql.Identifier(c) for c in insert_cols),
                geom_attrs=select_geom_attrs,
                agg=select_agg,
                geoms=select_geoms,
                geom_tbl=geom_tbl,
                agg_tbl=agg_tbl,
                key=key,
            )
        )
        cur.execute(sql.SQL("SELECT count(*) FROM {};").format(tbl))
        count = cur.fetchone()[0]
    conn.commit()
    return count


def materialize_choropleth_commune(postgis_credentials: dict) -> int:
    """Matérialise choropleth_commune = commune_geometry × agg_commune."""
    spec = _JoinSpec(
        target="choropleth_commune",
        geometry="commune_geometry",
        agg="agg_commune",
        join_key="code_commune",
        geom_columns=["code_commune", "nom_commune", "code_departement"],
    )
    with psycopg.connect(_dsn(postgis_credentials)) as conn:
        return _materialize(conn, spec)


def materialize_choropleth_departement(postgis_credentials: dict) -> int:
    """Matérialise choropleth_departement = departement_geometry × agg_departement."""
    spec = _JoinSpec(
        target="choropleth_departement",
        geometry="departement_geometry",
        agg="agg_departement",
        join_key="code_departement",
        geom_columns=["code_departement", "nom"],
    )
    with psycopg.connect(_dsn(postgis_credentials)) as conn:
        return _materialize(conn, spec)
