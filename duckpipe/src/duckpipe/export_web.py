"""Builders SQL des artefacts web statiques (cf. ADR-0013).

Chaque fonction matérialise une table DuckDB prête à être écrite en
GeoJSON/JSON par publish_web.py. Fonctions pures (con + noms de tables ->
nom de table), testables sur fixtures synthétiques.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

# Département dérivé du code commune : 3 caractères pour l'outre-mer (97x),
# 2 sinon (couvre naturellement la Corse 2A/2B).
DEPT_EXPR = (
    "CASE WHEN code_commune LIKE '97%' "
    "THEN substr(code_commune, 1, 3) ELSE substr(code_commune, 1, 2) END"
)

# NOTE : pas de simplification de géométries calculée ici. Les niveaux de
# détail viennent des contours pré-simplifiés Etalab (50m/100m/1000m),
# ingérés comme sources bronze : ST_CoverageSimplify s'est révélé non
# déterministe entre processus sur les données réelles (98 ou 109
# départements selon le lancement — même famille de problèmes que
# l'ADR-0008), et une simplification par feature créerait des interstices
# entre polygones voisins.

# Rattachement département -> région (découpage administratif 2016, stable
# depuis) : référentiel court et figé, embarqué plutôt qu'ingéré (une source
# de plus pour 101 lignes n'apporterait que de la fragilité réseau).
DEPT_TO_REGION: dict[str, tuple[str, str]] = {
    **dict.fromkeys(
        ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"],
        ("84", "Auvergne-Rhône-Alpes"),
    ),
    **dict.fromkeys(
        ["21", "25", "39", "58", "70", "71", "89", "90"],
        ("27", "Bourgogne-Franche-Comté"),
    ),
    **dict.fromkeys(["22", "29", "35", "56"], ("53", "Bretagne")),
    **dict.fromkeys(
        ["18", "28", "36", "37", "41", "45"], ("24", "Centre-Val de Loire")
    ),
    **dict.fromkeys(["2A", "2B"], ("94", "Corse")),
    **dict.fromkeys(
        ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"],
        ("44", "Grand Est"),
    ),
    **dict.fromkeys(["02", "59", "60", "62", "80"], ("32", "Hauts-de-France")),
    **dict.fromkeys(
        ["75", "77", "78", "91", "92", "93", "94", "95"], ("11", "Île-de-France")
    ),
    **dict.fromkeys(["14", "27", "50", "61", "76"], ("28", "Normandie")),
    **dict.fromkeys(
        ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"],
        ("75", "Nouvelle-Aquitaine"),
    ),
    **dict.fromkeys(
        ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"],
        ("76", "Occitanie"),
    ),
    **dict.fromkeys(["44", "49", "53", "72", "85"], ("52", "Pays de la Loire")),
    **dict.fromkeys(
        ["04", "05", "06", "13", "83", "84"], ("93", "Provence-Alpes-Côte d'Azur")
    ),
    "971": ("01", "Guadeloupe"),
    "972": ("02", "Martinique"),
    "973": ("03", "Guyane"),
    "974": ("04", "La Réunion"),
    "976": ("06", "Mayotte"),
}


def ensure_region_mapping(con: duckdb.DuckDBPyConnection) -> str:
    """Matérialise la table `region_mapping` (code_departement, code_region,
    nom_region) depuis le référentiel embarqué. Idempotent."""
    values = ", ".join(
        f"('{dept}', '{code}', '{nom.replace(chr(39), chr(39) * 2)}')"
        for dept, (code, nom) in sorted(DEPT_TO_REGION.items())
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE region_mapping AS
        SELECT * FROM (VALUES {values})
            AS v(code_departement, code_region, nom_region)
        """
    )
    return "region_mapping"


# Dimensions normalisées du score gold, exposées telles quelles dans les
# properties de la choroplèthe communale et les composantes des fiches.
SCORE_DIMENSIONS = [
    "n_prix",
    "n_transport",
    "n_access_fin",
    "n_risques",
    "n_tourisme",
    "n_securite",
    "n_services",
    "n_loisirs",
    "n_ensoleillement",
    "n_emploi",
    "n_proximite",
    "n_dpe",
]


def build_choropleth_communes(  # noqa: PLR0913 — une entrée par table jointe, convention Node
    con: duckdb.DuckDBPyConnection,
    commune_geom: str,
    commune_agg: str,
    commune_agg_type: str,
    score_territoire: str,
    *,
    out_table: str = "web_choropleth_communes",
) -> str:
    """Table choroplèthe communale : géométrie + toutes les métriques en
    colonnes plates (le writer GeoJSON les émet comme properties). Les
    switchers type_local et métrique de la carte deviennent des
    re-colorisations sans refetch. `commune_geom` désigne la variante de
    géométrie voulue (50m pour le LOD high, 1000m pour le mid).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            g.code_commune,
            g.nom_commune AS nom,
            {DEPT_EXPR.replace("code_commune", "g.code_commune")} AS code_departement,
            a.prix_m2_median,
            coalesce(a.nb_transactions, 0) AS nb_transactions,
            coalesce(a.fiable, false) AS fiable,
            m.prix_m2_median AS maison_prix_m2_median,
            coalesce(m.nb_transactions, 0) AS maison_nb_transactions,
            coalesce(m.fiable, false) AS maison_fiable,
            ap.prix_m2_median AS appart_prix_m2_median,
            coalesce(ap.nb_transactions, 0) AS appart_nb_transactions,
            coalesce(ap.fiable, false) AS appart_fiable,
            s.score_valeur,
            s.gap_pondere,
            round(s.gap, 3) AS gap,
            s.dpe_dominant,
            {", ".join(f"round(s.{d}, 3) AS {d}" for d in SCORE_DIMENSIONS)},
            g.geom
        FROM {commune_geom} g
        LEFT JOIN {commune_agg} a USING (code_commune)
        LEFT JOIN {commune_agg_type} m
            ON m.code_commune = g.code_commune AND m.type_local = 'Maison'
        LEFT JOIN {commune_agg_type} ap
            ON ap.code_commune = g.code_commune AND ap.type_local = 'Appartement'
        LEFT JOIN {score_territoire} s USING (code_commune)
        """
    )
    return out_table


def build_choropleth_departements(
    con: duckdb.DuckDBPyConnection,
    dept_geom: str,
    dept_agg: str,
    score_territoire: str,
    *,
    out_table: str = "web_choropleth_departements",
) -> str:
    """Table choroplèthe départementale : même forme que les communes,
    complétée par les agrégats du score au grain communal (médianes des
    communes scorées : score, gap et les 12 dimensions — mêmes noms `n_*`
    qu'à la maille communale pour que la carte bascule de maille sans
    adaptation). `dept_geom` désigne la variante de géométrie voulue (1000m
    pour le LOD low, 100m pour le mid). `code_region`/`nom_region` (additifs)
    permettent le drill-down régions -> départements côté client.
    """
    ensure_region_mapping(con)
    dims_medians = ", ".join(
        f"round(median(s.{d}), 3) AS {d}" for d in SCORE_DIMENSIONS
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE dept_score AS
        SELECT
            s.code_departement,
            count(*) AS nb_communes_scorees,
            round(median(s.score_valeur), 3) AS score_median,
            round(median(s.gap_pondere), 3) AS gap_pondere_median,
            {dims_medians}
        FROM {score_territoire} s
        GROUP BY s.code_departement
        """
    )
    dims_cols = ", ".join(f"sc.{d}" for d in SCORE_DIMENSIONS)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            g.code_departement,
            g.nom_departement AS nom,
            rm.code_region,
            rm.nom_region,
            t.prix_m2_median,
            coalesce(t.nb_transactions, 0) AS nb_transactions,
            coalesce(t.fiable, false) AS fiable,
            m.prix_m2_median AS maison_prix_m2_median,
            coalesce(m.nb_transactions, 0) AS maison_nb_transactions,
            coalesce(m.fiable, false) AS maison_fiable,
            ap.prix_m2_median AS appart_prix_m2_median,
            coalesce(ap.nb_transactions, 0) AS appart_nb_transactions,
            coalesce(ap.fiable, false) AS appart_fiable,
            sc.score_median,
            sc.gap_pondere_median,
            coalesce(sc.nb_communes_scorees, 0) AS nb_communes_scorees,
            {dims_cols},
            g.geom
        FROM {dept_geom} g
        LEFT JOIN {dept_agg} t
            ON t.code_departement = g.code_departement AND t.type_local IS NULL
        LEFT JOIN {dept_agg} m
            ON m.code_departement = g.code_departement AND m.type_local = 'Maison'
        LEFT JOIN {dept_agg} ap
            ON ap.code_departement = g.code_departement AND ap.type_local = 'Appartement'
        LEFT JOIN region_mapping rm ON rm.code_departement = g.code_departement
        LEFT JOIN dept_score sc ON sc.code_departement = g.code_departement
        """
    )
    return out_table


def build_choropleth_regions(
    con: duckdb.DuckDBPyConnection,
    region_geom: str,
    dvf: str,
    score_territoire: str,
    *,
    out_table: str = "web_choropleth_regions",
) -> str:
    """Table choroplèthe régionale : médianes recalculées depuis les
    TRANSACTIONS du millésime courant (une médiane de médianes communales
    serait biaisée par les petites communes), + agrégats du score au grain
    communal (médianes des communes scorées de la région).
    """
    ensure_region_mapping(con)
    # Grain transaction -> région : GROUPING SETS comme dept_agg (type_local
    # NULL = tous types), pivoté ensuite en colonnes plates tous/maison/appart.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE region_agg AS
        SELECT
            rm.code_region,
            d.type_local,
            count(*) AS nb_transactions,
            median(d.prix_m2) AS prix_m2_median,
            count(*) >= 5 AS fiable
        FROM {dvf} d
        JOIN region_mapping rm USING (code_departement)
        GROUP BY GROUPING SETS ((rm.code_region), (rm.code_region, d.type_local))
        """
    )
    dims_medians = ", ".join(
        f"round(median(s.{d}), 3) AS {d}" for d in SCORE_DIMENSIONS
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE region_score AS
        SELECT
            rm.code_region,
            count(*) AS nb_communes_scorees,
            round(median(s.score_valeur), 3) AS score_median,
            round(median(s.gap_pondere), 3) AS gap_pondere_median,
            {dims_medians}
        FROM {score_territoire} s
        JOIN region_mapping rm USING (code_departement)
        GROUP BY rm.code_region
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            g.code_region,
            g.nom_region AS nom,
            t.prix_m2_median,
            coalesce(t.nb_transactions, 0) AS nb_transactions,
            coalesce(t.fiable, false) AS fiable,
            m.prix_m2_median AS maison_prix_m2_median,
            coalesce(m.nb_transactions, 0) AS maison_nb_transactions,
            coalesce(m.fiable, false) AS maison_fiable,
            ap.prix_m2_median AS appart_prix_m2_median,
            coalesce(ap.nb_transactions, 0) AS appart_nb_transactions,
            coalesce(ap.fiable, false) AS appart_fiable,
            sc.score_median,
            sc.gap_pondere_median,
            coalesce(sc.nb_communes_scorees, 0) AS nb_communes_scorees,
            {", ".join(f"sc.{d}" for d in SCORE_DIMENSIONS)},
            g.geom
        FROM {region_geom} g
        LEFT JOIN region_agg t
            ON t.code_region = g.code_region AND t.type_local IS NULL
        LEFT JOIN region_agg m
            ON m.code_region = g.code_region AND m.type_local = 'Maison'
        LEFT JOIN region_agg ap
            ON ap.code_region = g.code_region AND ap.type_local = 'Appartement'
        LEFT JOIN region_score sc ON sc.code_region = g.code_region
        -- Le fichier Etalab contient aussi les COM (Polynésie, TAAF…) : on ne
        -- sert que les 18 régions du référentiel, les autres n'ont aucune donnée.
        WHERE g.code_region IN (SELECT DISTINCT code_region FROM region_mapping)
        """
    )
    return out_table


def build_evolution(
    con: duckdb.DuckDBPyConnection,
    commune_agg: str,
    year: int,
    millesime_tables: dict[int, str],
) -> str:
    """Table `web_evolution` : liste ordonnée {annee, prix_m2_median,
    nb_transactions} par commune, millésimes annexes + année courante."""
    union_parts = [
        f"SELECT code_commune, {annee} AS annee, prix_m2_median, nb_transactions FROM {table}"
        for annee, table in sorted(millesime_tables.items())
    ]
    union_parts.append(
        f"SELECT code_commune, {year} AS annee, prix_m2_median, nb_transactions "
        f"FROM {commune_agg}"
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE web_evolution AS
        SELECT code_commune,
               list({{'annee': annee, 'prix_m2_median': prix_m2_median,
                      'nb_transactions': nb_transactions}} ORDER BY annee) AS evolution
        FROM ({" UNION ALL ".join(union_parts)})
        GROUP BY code_commune
        """
    )
    return "web_evolution"


def build_fiches(  # noqa: PLR0913 — une entrée par table silver fusionnée
    con: duckdb.DuckDBPyConnection,
    commune_geom: str,
    commune_agg: str,
    commune_agg_type: str,
    score_territoire: str,
    web_evolution: str,
    revenus: str,
    emploi: str,
    commune_transport: str,
    equipements: str,
    securite: str,
    tourisme: str,
    risques: str,
    climat: str,
    proximite_metropole: str,
    avis_commune: str,
) -> str:
    """Table `web_fiches` : une ligne = une fiche commune (STRUCT imbriqués,
    sérialisés en JSON par COPY). Base = union des codes des contours et du
    score : aucune commune scorée sans fiche (des communes DVF fusionnées
    depuis peuvent manquer des contours du millésime suivant, ex. Marconne,
    et Saint-Martin 97127 est hors contours Etalab), et la recherche ne
    renvoie jamais 404. `score` est null pour les communes non scorées.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE web_fiches AS
        WITH base AS (
            SELECT code_commune, nom_commune FROM {commune_geom}
            UNION
            SELECT s.code_commune, s.nom_commune FROM {score_territoire} s
            WHERE s.code_commune NOT IN (SELECT code_commune FROM {commune_geom})
        )
        SELECT
            b.code_commune,
            b.nom_commune,
            {DEPT_EXPR.replace("code_commune", "b.code_commune")} AS code_departement,
            CASE WHEN a.code_commune IS NULL THEN NULL ELSE {{
                'median': a.prix_m2_median,
                'p25': a.prix_m2_p25,
                'p75': a.prix_m2_p75,
                'moyen': a.prix_m2_moyen,
                'nb_transactions': a.nb_transactions,
                'fiable': a.fiable,
                'maison': CASE WHEN m.code_commune IS NULL THEN NULL ELSE
                    {{'median': m.prix_m2_median, 'nb_transactions': m.nb_transactions}} END,
                'appartement': CASE WHEN ap.code_commune IS NULL THEN NULL ELSE
                    {{'median': ap.prix_m2_median, 'nb_transactions': ap.nb_transactions}} END
            }} END AS prix,
            e.evolution,
            CASE WHEN s.code_commune IS NULL THEN NULL ELSE {{
                'score_valeur': s.score_valeur,
                'gap': s.gap,
                'gap_pondere': s.gap_pondere,
                'dpe_dominant': s.dpe_dominant,
                'composantes': {{
                    'n_prix': s.n_prix, 'n_transport': s.n_transport,
                    'n_access_fin': s.n_access_fin, 'n_risques': s.n_risques,
                    'n_tourisme': s.n_tourisme, 'n_securite': s.n_securite,
                    'n_services': s.n_services, 'n_loisirs': s.n_loisirs,
                    'n_ensoleillement': s.n_ensoleillement, 'n_emploi': s.n_emploi,
                    'n_proximite': s.n_proximite, 'n_dpe': s.n_dpe
                }}
            }} END AS score,
            {{
                'revenu_median': r.revenu_median,
                'taux_chomage': em.taux_chomage,
                'taux_couverture_emploi': em.taux_couverture_emploi,
                'pop_active': em.pop_active,
                'densite_arrets_km2': t.densite_arrets_km2,
                'nb_arrets': t.nb_arrets,
                'nb_services_sante': eq.nb_services_sante,
                'nb_loisirs_culture': eq.nb_loisirs_culture,
                'taux_delinquance_global': se.taux_delinquance_global,
                'insee_pop': se.insee_pop,
                'part_residences_secondaires': tou.part_residences_secondaires,
                'nb_arretes_catnat': ri.nb_arretes_catnat,
                'ensoleillement_h_an': cl.ensoleillement_h_an,
                'temperature_moy_annuelle': cl.temperature_moy_annuelle,
                'dist_metropole_km': p.dist_metropole_km,
                'nom_metropole': p.nom_metropole,
                'surface_km2': g.surface_km2
            }} AS indicateurs,
            CASE WHEN av.code_commune IS NULL THEN NULL ELSE {{
                'n_avis': av.n_avis,
                'sentiment_global': round(av.sentiment_global, 2)::DOUBLE,
                'periode': {{'debut': av.date_min, 'fin': av.date_max}},
                'low_data': av.low_data,
                'mini_cloud': av.wordcloud_preview
            }} END AS avis
        FROM base b
        LEFT JOIN {commune_geom} g USING (code_commune)
        LEFT JOIN {commune_agg} a USING (code_commune)
        LEFT JOIN {commune_agg_type} m
            ON m.code_commune = b.code_commune AND m.type_local = 'Maison'
        LEFT JOIN {commune_agg_type} ap
            ON ap.code_commune = b.code_commune AND ap.type_local = 'Appartement'
        LEFT JOIN {score_territoire} s USING (code_commune)
        LEFT JOIN {web_evolution} e USING (code_commune)
        LEFT JOIN {revenus} r USING (code_commune)
        LEFT JOIN {emploi} em USING (code_commune)
        LEFT JOIN {commune_transport} t USING (code_commune)
        LEFT JOIN {equipements} eq USING (code_commune)
        LEFT JOIN {securite} se USING (code_commune)
        LEFT JOIN {tourisme} tou USING (code_commune)
        LEFT JOIN {risques} ri USING (code_commune)
        LEFT JOIN {climat} cl USING (code_commune)
        LEFT JOIN {proximite_metropole} p USING (code_commune)
        LEFT JOIN {avis_commune} av USING (code_commune)
        """
    )
    return "web_fiches"


def create_avis_stub(con: duckdb.DuckDBPyConnection, *, out_table: str = "avis_commune") -> str:
    """Crée une table ``avis_commune`` vide et typée.

    Permet à ``publish-web`` de tourner quand l'étape NLP n'a jamais été
    produite : les fiches se construisent avec ``avis: null`` et aucun artefact
    avis n'est écrit. Le typage doit correspondre à la sortie de
    ``pipelines/avis.py`` pour que les jointures/COPY aval fonctionnent.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            CAST(NULL AS VARCHAR) AS code_commune,
            CAST(NULL AS VARCHAR) AS nom_ville,
            CAST(NULL AS BIGINT) AS n_avis,
            CAST(NULL AS DATE) AS date_min,
            CAST(NULL AS DATE) AS date_max,
            CAST(NULL AS DOUBLE) AS sentiment_global,
            CAST(NULL AS BOOLEAN) AS low_data,
            CAST(NULL AS STRUCT(theme VARCHAR, n_segments BIGINT, pct_positive DOUBLE,
                                pct_negative DOUBLE, score DOUBLE)[]) AS themes,
            CAST(NULL AS STRUCT(word VARCHAR, weight BIGINT, sentiment VARCHAR,
                                themes VARCHAR[])[]) AS wordcloud,
            CAST(NULL AS STRUCT(word VARCHAR, weight BIGINT, sentiment VARCHAR)[])
                AS wordcloud_preview,
            CAST(NULL AS STRUCT("text" VARCHAR, "label" VARCHAR, theme VARCHAR,
                                mois VARCHAR, "source" VARCHAR)[]) AS verbatims
        WHERE false
        """
    )
    return out_table


def build_avis(
    con: duckdb.DuckDBPyConnection,
    avis_commune: str,
    *,
    out_table: str = "web_avis",
) -> str:
    """Table ``web_avis`` : une ligne par commune ayant des avis, prête pour le
    JSON par département. Le sentiment par thème est masqué (NULL) pour les
    communes ``low_data`` (< 10 avis) — non fiable statistiquement (maquette) ;
    le gold conserve la donnée complète pour l'analyse.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            code_commune,
            {DEPT_EXPR} AS code_departement,
            nom_ville,
            n_avis,
            {{'debut': date_min, 'fin': date_max}} AS periode,
            round(sentiment_global, 2)::DOUBLE AS sentiment_global,
            low_data,
            CASE WHEN low_data THEN NULL ELSE themes END AS themes,
            wordcloud,
            verbatims,
            'Ville-idéale' AS source
        FROM {avis_commune}
        """
    )
    return out_table


def build_avis_index(
    con: duckdb.DuckDBPyConnection,
    web_avis: str,
    commune_geom: str,
    *,
    out_table: str = "web_avis_index",
) -> str:
    """Table `web_avis_index` : les communes couvertes par l'analyse d'avis,
    avec leur centre (centroïde du contour) — permet à la carte de poser les
    marqueurs « avis » sans télécharger les analyses complètes ni embarquer
    de données locales côté front.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            a.code_commune AS c,
            a.nom_ville AS n,
            a.n_avis,
            CAST(round(ST_X(ST_Centroid(g.geom)), 4) AS DOUBLE) AS lng,
            CAST(round(ST_Y(ST_Centroid(g.geom)), 4) AS DOUBLE) AS lat
        FROM {web_avis} a
        JOIN {commune_geom} g USING (code_commune)
        ORDER BY a.code_commune
        """
    )
    return out_table


def build_search_index(
    con: duckdb.DuckDBPyConnection,
    commune_geom: str,
    commune_agg: str,
    score_territoire: str,
) -> str:
    """Table `web_search_index` : clés courtes (c=code, n=nom, d=dept,
    p=prix médian arrondi, s=score arrondi) pour un index client léger.
    Même base que les fiches (contours ∪ score) : tout ce qui est cherchable
    a une fiche, et réciproquement."""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE web_search_index AS
        WITH base AS (
            SELECT code_commune, nom_commune FROM {commune_geom}
            UNION
            SELECT s.code_commune, s.nom_commune FROM {score_territoire} s
            WHERE s.code_commune NOT IN (SELECT code_commune FROM {commune_geom})
        )
        SELECT
            b.code_commune AS c,
            b.nom_commune AS n,
            {DEPT_EXPR.replace("code_commune", "b.code_commune")} AS d,
            CAST(round(a.prix_m2_median) AS INTEGER) AS p,
            round(s.score_valeur, 2) AS s
        FROM base b
        LEFT JOIN {commune_agg} a USING (code_commune)
        LEFT JOIN {score_territoire} s USING (code_commune)
        ORDER BY b.code_commune
        """
    )
    return "web_search_index"


def build_classement(
    con: duckdb.DuckDBPyConnection, score_territoire: str, *, top_n: int = 100
) -> str:
    """Table `web_classement` : top des communes sous-cotées par gap pondéré."""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE web_classement AS
        SELECT
            CAST(row_number() OVER (ORDER BY gap_pondere DESC) AS INTEGER) AS rang,
            code_commune,
            nom_commune,
            code_departement,
            prix_m2_median,
            round(score_valeur, 4) AS score_valeur,
            round(gap_pondere, 4) AS gap_pondere
        FROM {score_territoire}
        ORDER BY gap_pondere DESC
        LIMIT {top_n}
        """
    )
    return "web_classement"


def build_points_sample(
    con: duckdb.DuckDBPyConnection,
    dvf: str,
    *,
    n: int = 100_000,
    out_table: str = "web_points_sample",
) -> str:
    """Table `web_points_sample` : échantillon de mutations géolocalisées pour
    la heatmap/isolignes de la carte. Reservoir sampling REPEATABLE : tirage
    reproductible d'un run à l'autre à données constantes (ADR-0007, threads=1).
    Coordonnées arrondies à 4 décimales (~11 m), type compacté M/A.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            CAST(round(longitude, 4) AS DOUBLE) AS lon,
            CAST(round(latitude, 4) AS DOUBLE) AS lat,
            CAST(round(prix_m2) AS INTEGER) AS prix,
            CASE type_local WHEN 'Maison' THEN 'M' ELSE 'A' END AS t
        FROM {dvf}
        USING SAMPLE reservoir({n} ROWS) REPEATABLE (42)
        """
    )
    return out_table


# --- Payloads charts/ (enveloppes JSON, pas des tables geo) -------------------
# Portés de webapp_export/export_charts.py (branche feat/webapp-export) pour que
# chaque run publié embarque ses artefacts charts : contrats identiques à ceux
# consommés par webapp/src/lib/charts.ts (schema_version 1 par artefact).


def build_stats_communes(  # noqa: PLR0913 — une entrée par table silver jointe
    con: duckdb.DuckDBPyConnection,
    score_territoire: str,
    revenus: str,
    emploi: str,
    climat: str,
    tourisme: str,
    proximite_metropole: str,
    commune_transport: str,
    *,
    year: int,
) -> dict:
    """Payload `charts/stats_communes.json` : valeurs brutes par commune scorée
    (périmètre gold), jointures silver par code_commune."""
    rows = con.execute(
        f"""
        SELECT
            s.code_commune,
            s.nom_commune                          AS nom,
            s.code_departement                     AS dep,
            round(s.prix_m2_median)                AS prix_m2_median,
            s.nb_transactions,
            round(e.taux_chomage, 2)               AS taux_chomage,
            round(e.taux_couverture_emploi, 2)     AS taux_couverture_emploi,
            round(r.revenu_median)                 AS revenu_median,
            round(c.ensoleillement_h_an)           AS ensoleillement_h_an,
            round(c.jours_ensoleilles)             AS jours_ensoleilles,
            round(c.temperature_moy_annuelle, 1)   AS temperature_moy_annuelle,
            round(t.part_residences_secondaires, 4) AS part_residences_secondaires,
            round(p.dist_metropole_km, 1)          AS dist_metropole_km,
            tr.nb_arrets,
            round(tr.densite_arrets_km2, 2)        AS densite_arrets_km2
        FROM {score_territoire} s
        LEFT JOIN {emploi} e USING (code_commune)
        LEFT JOIN {revenus} r USING (code_commune)
        LEFT JOIN {climat} c USING (code_commune)
        LEFT JOIN {tourisme} t USING (code_commune)
        LEFT JOIN {proximite_metropole} p USING (code_commune)
        LEFT JOIN {commune_transport} tr USING (code_commune)
        ORDER BY s.code_commune
        """
    ).fetchall()
    cols = [d[0] for d in con.description]
    return {
        "schema_version": 1,
        "year": year,
        "communes": [dict(zip(cols, row, strict=True)) for row in rows],
    }


def build_prix_distribution(
    con: duckdb.DuckDBPyConnection, dvf: str, *, year: int, bins: int = 60
) -> dict:
    """Payload `charts/prix_distribution.json` : histogrammes du prix au m²
    (tous/maison/appartement) sur bins uniformes de 0 au p99 global — le
    silver est déjà clippé p1-p99 par département×type, la borne rend juste
    l'axe lisible."""
    p99 = con.execute(
        f"SELECT quantile_cont(prix_m2, 0.99) FROM {dvf}"
    ).fetchone()[0]
    hi = float(round(p99, -2))  # arrondi à la centaine : bords de bins propres
    width = hi / bins

    def counts(where: str) -> list[int]:
        rows = con.execute(
            f"""
            SELECT least(floor(prix_m2 / {width}), {bins - 1})::INT AS b, count(*)
            FROM {dvf}
            WHERE prix_m2 <= {hi} {where}
            GROUP BY b ORDER BY b
            """
        ).fetchall()
        out = [0] * bins
        for b, nb in rows:
            out[b] = nb
        return out

    return {
        "schema_version": 1,
        "year": year,
        "bin_edges": [round(i * width) for i in range(bins + 1)],
        "series": {
            "tous": counts(""),
            "maison": counts("AND type_local = 'Maison'"),
            "appartement": counts("AND type_local = 'Appartement'"),
        },
    }


def build_prix_series(
    con: duckdb.DuckDBPyConnection,
    commune_agg: str,
    year: int,
    millesime_tables: dict[int, str],
    *,
    min_transactions: int = 5,
) -> dict:
    """Payload `charts/prix_series.json` : médianes annuelles par commune
    (null si < min_transactions ventes) + série nationale de référence.

    La médiane nationale est la médiane pondérée (par nb de ventes) des
    médianes communales : les millésimes annexes ne conservent que l'agrégat
    communal (silver commune_prix), pas les transactions. Méthode identique
    sur toutes les années — année courante comprise — pour une série homogène.
    """
    year_tables = dict(sorted({**millesime_tables, year: commune_agg}.items()))
    years = list(year_tables)

    national: list[float | None] = []
    communes: dict[str, list[float | None]] = {}
    for i, (annee, table) in enumerate(year_tables.items()):
        med = con.execute(
            f"""
            WITH cum AS (
                SELECT prix_m2_median AS v,
                       sum(nb_transactions) OVER (ORDER BY prix_m2_median) AS cw,
                       sum(nb_transactions) OVER () AS tw
                FROM {table}
            )
            SELECT round(min(v)) FROM cum WHERE cw >= tw / 2
            """
        ).fetchone()[0]
        national.append(med)
        rows = con.execute(
            f"""
            SELECT code_commune, round(prix_m2_median)
            FROM {table} WHERE nb_transactions >= {min_transactions}
            """
        ).fetchall()
        for code, value in rows:
            communes.setdefault(code, [None] * len(years))[i] = value

    return {
        "schema_version": 1,
        "years": years,
        "national": national,
        "communes": communes,
    }


def build_score_geojson_compat(
    con: duckdb.DuckDBPyConnection,
    choropleth_communes: str,
    *,
    out_table: str = "web_score_compat",
) -> str:
    """Table du `v1/score.geojson` de compatibilité, DÉPRÉCIÉ (cf. ADR-0014).

    Reprend le contrat de l'ancien script webapp_export/export_score_geojson.py
    (supprimé) le temps que la webapp migre vers meta.json + runs/ : communes
    scorées uniquement, mêmes noms de properties. À retirer une fois la
    migration du front confirmée en prod.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            code_commune,
            nom,
            code_departement AS dep,
            CAST(round(prix_m2_median) AS INTEGER) AS prix,
            nb_transactions,
            dpe_dominant AS dpe,
            round(score_valeur, 3) AS score_valeur,
            gap,
            round(gap_pondere, 3) AS gap_pondere,
            {", ".join(SCORE_DIMENSIONS)},
            geom
        FROM {choropleth_communes}
        WHERE score_valeur IS NOT NULL
        """
    )
    return out_table
