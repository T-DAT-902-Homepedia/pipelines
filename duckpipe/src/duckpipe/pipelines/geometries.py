from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb


def clean_geometries(con: duckdb.DuckDBPyConnection, table: str = "commune_geom") -> None:
    """Écarte (en place) les géométries inutilisables : `geom` NULL ou `surface_km2`
    NULL/NaN/<=0 — ces contours dégénérés casseraient densités et jointures spatiales.

    Adaptation de exploration/src/preprocess.py::clean_geometries (rapport de
    rejets non persisté ici, cf. décision inline du portage PR1).
    """
    surf_invalide = "surface_km2 IS NULL OR isnan(surface_km2) OR surface_km2 <= 0"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT * FROM {table}
        WHERE geom IS NOT NULL AND NOT ({surf_invalide})
        """
    )


def geometries(con: duckdb.DuckDBPyConnection, communes_raw: str, depts_raw: str) -> dict[str, str]:
    """Adaptation de `exploration/src/ingest.py::ensure_geometries`.

    Produit `commune_geom` (code_commune, nom_commune, geom, surface_km2) et
    `dept_geom` (code_departement, nom_departement, geom) à partir des GeoJSON
    Etalab. `surface_km2` est pré-calculée (ST_Area_Spheroid) pour les densités.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE commune_geom AS
        SELECT
            lpad(CAST(code AS VARCHAR), 5, '0') AS code_commune,
            nom AS nom_commune,
            geom,
            NULLIF(ST_Area_Spheroid(geom) / 1e6, 0) AS surface_km2
        FROM {communes_raw}
        """
    )
    clean_geometries(con, "commune_geom")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE dept_geom AS
        SELECT lpad(CAST(code AS VARCHAR), 2, '0') AS code_departement,
               nom AS nom_departement, geom
        FROM {depts_raw}
        """
    )
    return {"commune_geom": "commune_geom", "dept_geom": "dept_geom"}


def geometries_web(  # noqa: PLR0913 — une entrée par variante + les référentiels
    con: duckdb.DuckDBPyConnection,
    communes_1000m_raw: str,
    depts_100m_raw: str,
    depts_1000m_raw: str,
    commune_geom: str,
    dept_geom: str,
) -> dict[str, str]:
    """Variantes pré-simplifiées Etalab pour les LOD des choroplèthes web
    (ADR-0013) : topologie de couverture garantie par le producteur, aucune
    simplification calculée ici (ST_CoverageSimplify s'est révélé non
    déterministe entre processus, dans la lignée de l'ADR-0008).

    Chaque variante est alignée sur le référentiel nettoyé (les fichiers
    1000m incluent 86 collectivités du Pacifique — Polynésie,
    Wallis-et-Futuna… — que clean_geometries écarte du 50m et pour
    lesquelles aucune source de données n'existe).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE commune_geom_1000m AS
        SELECT lpad(CAST(code AS VARCHAR), 5, '0') AS code_commune,
               nom AS nom_commune, geom
        FROM {communes_1000m_raw}
        WHERE lpad(CAST(code AS VARCHAR), 5, '0')
              IN (SELECT code_commune FROM {commune_geom})
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE dept_geom_100m AS
        SELECT lpad(CAST(code AS VARCHAR), 2, '0') AS code_departement,
               nom AS nom_departement, geom
        FROM {depts_100m_raw}
        WHERE lpad(CAST(code AS VARCHAR), 2, '0')
              IN (SELECT code_departement FROM {dept_geom})
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE dept_geom_1000m AS
        SELECT lpad(CAST(code AS VARCHAR), 2, '0') AS code_departement,
               nom AS nom_departement, geom
        FROM {depts_1000m_raw}
        WHERE lpad(CAST(code AS VARCHAR), 2, '0')
              IN (SELECT code_departement FROM {dept_geom})
        """
    )
    return {
        "commune_geom_1000m": "commune_geom_1000m",
        "dept_geom_100m": "dept_geom_100m",
        "dept_geom_1000m": "dept_geom_1000m",
    }


geometries_pipeline = Pipeline(
    nodes=[
        Node(
            func=geometries,
            inputs=["communes_raw", "depts_raw"],
            outputs=["commune_geom", "dept_geom"],
            name="geometries",
        ),
    ]
)

# Pipeline séparé : les variantes LOD ne servent qu'à publish-web et
# dépendent du référentiel nettoyé produit par geometries_pipeline.
geometries_web_pipeline = Pipeline(
    nodes=[
        Node(
            func=geometries_web,
            inputs=[
                "communes_1000m_raw",
                "depts_100m_raw",
                "depts_1000m_raw",
                "commune_geom",
                "dept_geom",
            ],
            outputs=["commune_geom_1000m", "dept_geom_100m", "dept_geom_1000m"],
            name="geometries_web",
        ),
    ]
)
