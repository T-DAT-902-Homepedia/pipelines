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
    *,
    out_table: str = "web_choropleth_departements",
) -> str:
    """Table choroplèthe départementale : même forme que les communes
    (sans score, calculé au grain communal uniquement). `dept_geom` désigne
    la variante de géométrie voulue (1000m pour le LOD low, 100m pour le mid).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            g.code_departement,
            g.nom_departement AS nom,
            t.prix_m2_median,
            coalesce(t.nb_transactions, 0) AS nb_transactions,
            coalesce(t.fiable, false) AS fiable,
            m.prix_m2_median AS maison_prix_m2_median,
            coalesce(m.nb_transactions, 0) AS maison_nb_transactions,
            coalesce(m.fiable, false) AS maison_fiable,
            ap.prix_m2_median AS appart_prix_m2_median,
            coalesce(ap.nb_transactions, 0) AS appart_nb_transactions,
            coalesce(ap.fiable, false) AS appart_fiable,
            g.geom
        FROM {dept_geom} g
        LEFT JOIN {dept_agg} t
            ON t.code_departement = g.code_departement AND t.type_local IS NULL
        LEFT JOIN {dept_agg} m
            ON m.code_departement = g.code_departement AND m.type_local = 'Maison'
        LEFT JOIN {dept_agg} ap
            ON ap.code_departement = g.code_departement AND ap.type_local = 'Appartement'
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
            }} AS indicateurs
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
        """
    )
    return "web_fiches"


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
