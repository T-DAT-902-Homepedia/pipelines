"""Score de valeur réelle par commune (gold).

Adaptation SQL de `exploration/notebooks/exploration.py::section_score`
(référence pandas/scikit-learn) : fusion des dimensions silver, normalisation
p01-p99 + minmax (`_norm`), score composite pondéré avec renormalisation des
poids sur les composantes disponibles, gap qualité-prix et gap pondéré par
l'accessibilité financière.

Choix de fidélité :
- pandas `Series.quantile`/`median` et DuckDB `quantile_cont`/`median` font la
  même interpolation linéaire en ignorant les manquants : les résultats sont
  identiques au bruit flottant près (vérifié contre la référence, cf. tests) ;
- le clip p01-p99 suivi d'un minmax équivaut à `(clip(x) - p01) / (p99 - p01)`
  car min(x) ≤ p01 et max(x) ≥ p99 par définition des quantiles ;
- l'étiquette DPE dominante départage les ex æquo par ordre alphabétique (le
  notebook laissait ce cas non déterministe).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# Poids du score composite (somme = 1). n_access_fin est exclu du score
# (contamination circulaire : le prix serait pénalisé deux fois) ; il pondère
# uniquement le gap final. n_dpe est informatif, hors score.
POIDS = {
    "n_emploi": 0.30,
    "n_proximite": 0.18,
    "n_transport": 0.15,
    "n_securite": 0.12,
    "n_services": 0.12,
    "n_loisirs": 0.07,
    "n_ensoleillement": 0.04,
    "n_risques": 0.01,
    "n_tourisme": 0.01,
}


def _norm(col: str, *, invert: bool = False) -> str:
    """SQL de `_norm` du notebook : clip aux quantiles 1-99 % puis minmax."""
    p01 = f"quantile_cont({col}, 0.01) OVER ()"
    p99 = f"quantile_cont({col}, 0.99) OVER ()"
    clipped = f"greatest({p01}, least({p99}, {col}))"
    norm = f"(({clipped}) - ({p01})) / nullif(({p99}) - ({p01}), 0)"
    return f"1 - ({norm})" if invert else norm


def _minmax(col: str) -> str:
    """SQL de `minmax_scale` sans clip (transport, rang DPE)."""
    lo = f"min({col}) OVER ()"
    hi = f"max({col}) OVER ()"
    return f"(({col}) - ({lo})) / nullif(({hi}) - ({lo}), 0)"


def score(  # noqa: PLR0913 — un paramètre par table silver fusionnée, convention Node
    con: duckdb.DuckDBPyConnection,
    commune_agg: str,
    commune_transport: str,
    dpe: str,
    revenus: str,
    risques: str,
    tourisme: str,
    securite: str,
    equipements: str,
    climat: str,
    emploi: str,
    proximite_metropole: str,
) -> str:
    """Produit la table gold `score_territoire` (communes fiables uniquement)."""
    poids_num = " + ".join(f"{w} * coalesce({n}, 0)" for n, w in POIDS.items())
    poids_den = " + ".join(f"{w} * CAST({n} IS NOT NULL AS INTEGER)" for n, w in POIDS.items())

    con.execute(
        f"""
        CREATE OR REPLACE TABLE score_territoire AS
        WITH dpe_commune AS (
            SELECT code_commune, etiquette_dpe AS dpe_dominant
            FROM (
                SELECT code_commune, etiquette_dpe,
                       row_number() OVER (PARTITION BY code_commune
                                          ORDER BY count(*) DESC, etiquette_dpe) AS rk
                FROM {dpe} GROUP BY code_commune, etiquette_dpe
            ) WHERE rk = 1
        ),
        base AS (
            SELECT c.code_commune, c.nom_commune, c.code_departement,
                   c.nb_transactions, c.prix_m2_median,
                   t.densite_arrets_km2,
                   d.dpe_dominant,
                   r.revenu_median,
                   k.nb_arretes_catnat,
                   tou.part_residences_secondaires,
                   s.taux_delinquance_global,
                   CAST(s.insee_pop AS DOUBLE) AS insee_pop,
                   e.nb_services_sante, e.nb_loisirs_culture,
                   cl.ensoleillement_h_an,
                   em.taux_chomage, em.taux_couverture_emploi,
                   p.dist_metropole_km
            FROM {commune_agg} c
            LEFT JOIN {commune_transport} t USING (code_commune)
            LEFT JOIN dpe_commune d USING (code_commune)
            LEFT JOIN {revenus} r USING (code_commune)
            LEFT JOIN {risques} k USING (code_commune)
            LEFT JOIN {tourisme} tou USING (code_commune)
            LEFT JOIN {securite} s USING (code_commune)
            LEFT JOIN {equipements} e USING (code_commune)
            LEFT JOIN {climat} cl USING (code_commune)
            LEFT JOIN {emploi} em USING (code_commune)
            LEFT JOIN {proximite_metropole} p USING (code_commune)
            WHERE c.fiable
        ),
        -- Imputations : insee_pop par médiane départementale (secret statistique
        -- SSMSI ~30 % des communes), les autres par médiane globale.
        imputed AS (
            SELECT *,
                coalesce(insee_pop,
                         median(insee_pop) OVER (PARTITION BY code_departement))
                    AS insee_pop_imp,
                prix_m2_median / revenu_median AS inaccessibilite_brute
            FROM base
        ),
        prepared AS (
            SELECT *,
                coalesce(inaccessibilite_brute,
                         median(inaccessibilite_brute) OVER ()) AS inaccessibilite,
                ln(1 + coalesce(densite_arrets_km2, 0)) AS log_transport,
                ln(1 + coalesce(nb_arretes_catnat, 0)) AS log_risques,
                coalesce(part_residences_secondaires, 0) AS tourisme_brut,
                ln(1 + coalesce(taux_delinquance_global, 0)) AS log_securite,
                ln(1 + coalesce(nb_services_sante / nullif(insee_pop_imp, 0) * 1000, 0))
                    AS log_services,
                ln(1 + coalesce(nb_loisirs_culture / nullif(insee_pop_imp, 0) * 1000, 0))
                    AS log_loisirs,
                coalesce(ensoleillement_h_an,
                         median(ensoleillement_h_an) OVER ()) AS ensoleillement_impute,
                ln(1 + coalesce(taux_chomage, median(taux_chomage) OVER ())) AS log_chomage,
                ln(1 + coalesce(taux_couverture_emploi,
                                median(taux_couverture_emploi) OVER ())) AS log_couverture,
                coalesce(dist_metropole_km,
                         median(dist_metropole_km) OVER ()) AS dist_metropole_imputee,
                CAST(coalesce(CASE dpe_dominant
                    WHEN 'A' THEN 6 WHEN 'B' THEN 5 WHEN 'C' THEN 4 WHEN 'D' THEN 3
                    WHEN 'E' THEN 2 WHEN 'F' THEN 1 WHEN 'G' THEN 0 END, 3) AS DOUBLE)
                    AS dpe_rang
            FROM imputed
        ),
        normalized AS (
            SELECT code_commune, nom_commune, code_departement,
                   nb_transactions, prix_m2_median, dpe_dominant,
                   {_norm("prix_m2_median")} AS n_prix,
                   {_minmax("log_transport")} AS n_transport,
                   {_norm("inaccessibilite", invert=True)} AS n_access_fin,
                   {_norm("log_risques", invert=True)} AS n_risques,
                   {_norm("tourisme_brut")} AS n_tourisme,
                   {_norm("log_securite", invert=True)} AS n_securite,
                   {_norm("log_services")} AS n_services,
                   {_norm("log_loisirs")} AS n_loisirs,
                   {_norm("ensoleillement_impute")} AS n_ensoleillement,
                   0.5 * ({_norm("log_chomage", invert=True)})
                     + 0.5 * ({_norm("log_couverture")}) AS n_emploi,
                   {_norm("dist_metropole_imputee", invert=True)} AS n_proximite,
                   {_minmax("dpe_rang")} AS n_dpe
            FROM prepared
        )
        SELECT *,
               ({poids_num}) / nullif({poids_den}, 0) AS score_valeur,
               (({poids_num}) / nullif({poids_den}, 0)) - n_prix AS gap,
               ((({poids_num}) / nullif({poids_den}, 0)) - n_prix) * n_access_fin
                   AS gap_pondere
        FROM normalized
        """
    )
    return "score_territoire"


score_pipeline = Pipeline(
    nodes=[
        Node(
            func=score,
            inputs=[
                "commune_agg",
                "commune_transport",
                "dpe",
                "revenus",
                "risques",
                "tourisme",
                "securite",
                "equipements",
                "climat",
                "emploi",
                "proximite_metropole",
            ],
            outputs=["score_territoire"],
            name="score",
        ),
    ]
)
