"""Data quality : profilage et validation génériques sur tables DuckDB.

Adaptation de `exploration/src/quality.py` (module déjà générique, agnostique
de la source). Deux briques réutilisables sur n'importe quelle table :

- `profile()`  : portrait d'une table (volumétrie, % manquant, cardinalités).
- `validate()` : applique une liste de règles nommées et compte les violations.

Rien n'est supprimé ici (contrairement à `preprocess.clean_*`) : c'est un
diagnostic, pas un nettoyage.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
        [name],
    ).fetchone()
    return bool(row and row[0])


def profile(
    con: duckdb.DuckDBPyConnection, table: str, numeric_cols: list[str] | None = None
) -> list[dict]:
    """Profil colonne par colonne d'une table : type, % manquant, cardinalité.

    Pour les colonnes numériques listées dans `numeric_cols`, ajoute min /
    médiane / max. Renvoie une liste de dicts (une ligne par colonne).
    """
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    cols = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()

    rows = []
    for name, dtype in cols:
        n_null = con.execute(f'SELECT count(*) FROM {table} WHERE "{name}" IS NULL').fetchone()[0]
        n_distinct = con.execute(f'SELECT count(DISTINCT "{name}") FROM {table}').fetchone()[0]
        rec = {
            "colonne": name,
            "type": dtype,
            "%_manquant": round(100 * n_null / total, 2) if total else 0.0,
            "cardinalite": n_distinct,
        }
        if numeric_cols and name in numeric_cols:
            mn, md, mx = con.execute(
                f'SELECT min("{name}"), median("{name}"), max("{name}") FROM {table}'
            ).fetchone()
            rec |= {"min": mn, "mediane": md, "max": mx}
        rows.append(rec)
    return rows


@dataclass
class Rule:
    """Règle de validation : `name` documente l'attente, `where` décrit la VIOLATION.

    `where` est une condition SQL qui sélectionne les lignes *invalides*.
    0 ligne en violation = règle respectée. `critical=True` signale une règle
    dont l'échec doit alerter fortement.
    """

    name: str
    where: str
    critical: bool = False


def validate(con: duckdb.DuckDBPyConnection, table: str, rules: list[Rule]) -> list[dict]:
    """Exécute les règles sur `table` et renvoie le compte de violations par règle."""
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    out = []
    for rule in rules:
        n_bad = con.execute(f"SELECT count(*) FROM {table} WHERE {rule.where}").fetchone()[0]
        out.append(
            {
                "regle": rule.name,
                "violations": n_bad,
                "%": round(100 * n_bad / total, 3) if total else 0.0,
                "critique": rule.critical,
                "statut": "OK" if n_bad == 0 else "KO",
            }
        )
    return out


def coverage(
    con: duckdb.DuckDBPyConnection,
    sources: list[dict],
    ref_table: str = "commune_geom",
    ref_key: str = "code_commune",
) -> list[dict]:
    """Tableau de couverture géographique consolidé, une ligne par source.

    Pour chaque source décrite par un dict {table, label, [seuil], [note]}, on
    compte les communes distinctes renseignées et le taux vs référentiel.
    """
    ref_n = con.execute(f"SELECT count(DISTINCT {ref_key}) FROM {ref_table}").fetchone()[0]

    rows = []
    for source in sources:
        if not table_exists(con, source["table"]):
            rows.append(
                {
                    "source": source["label"],
                    "communes": 0,
                    "taux_%": 0.0,
                    "alerte": "table absente (source non exécutée)",
                }
            )
            continue
        n = con.execute(f"SELECT count(DISTINCT {ref_key}) FROM {source['table']}").fetchone()[0]
        taux = round(100 * n / ref_n, 1) if ref_n else 0.0
        seuil = source.get("seuil")
        alerte = ""
        if seuil is not None and taux < seuil:
            alerte = source.get("note", "couverture faible")
        rows.append({"source": source["label"], "communes": n, "taux_%": taux, "alerte": alerte})
    return rows


def match_rate(
    con: duckdb.DuckDBPyConnection, left: str, right: str, key: str = "code_commune"
) -> dict:
    """Taux d'appariement d'une jointure sur `key` (détecte les clés orphelines)."""
    n_left = con.execute(f"SELECT count(DISTINCT {key}) FROM {left}").fetchone()[0]
    n_match = con.execute(
        f"SELECT count(DISTINCT l.{key}) FROM {left} l JOIN {right} r ON l.{key} = r.{key}"
    ).fetchone()[0]
    return {
        "left": left,
        "right": right,
        "cles_left": n_left,
        "appariees": n_match,
        "orphelines": n_left - n_match,
        "taux": round(100 * n_match / n_left, 2) if n_left else 0.0,
    }


# Règles de validation post-nettoyage sur `dvf`, reprises de exploration/src/quality.py.
DVF_RULES = [
    Rule("prix/m2 dans les bornes plausibles", "prix_m2 < 100 OR prix_m2 > 50000", critical=True),
    Rule(
        "surface batie dans les bornes plausibles",
        "surface_bati < 9 OR surface_bati > 1000",
        critical=True,
    ),
    Rule("coordonnees presentes", "longitude IS NULL OR latitude IS NULL", critical=True),
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("valeur fonciere strictement positive", "valeur_fonciere <= 0"),
    Rule(
        "type de local attendu (Maison/Appartement)",
        "type_local NOT IN ('Maison', 'Appartement')",
    ),
]

GEOM_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("geometrie presente", "geom IS NULL", critical=True),
    Rule(
        "surface strictement positive",
        "surface_km2 IS NULL OR isnan(surface_km2) OR surface_km2 <= 0",
        critical=True,
    ),
]

TRANSPORT_RULES = [
    Rule("nombre d'arrets non negatif", "nb_arrets < 0", critical=True),
    Rule("densite non negative", "densite_arrets_km2 < 0", critical=True),
    Rule(
        "densite coherente avec nb_arrets/surface",
        "surface_km2 > 0 AND densite_arrets_km2 IS NOT NULL "
        "AND abs(densite_arrets_km2 - nb_arrets / surface_km2) > 1e-6",
    ),
]

REVENUS_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("revenu median present", "revenu_median IS NULL", critical=True),
    Rule(
        "revenu median dans les bornes plausibles",
        "revenu_median < 5000 OR revenu_median > 80000",
        critical=True,
    ),
    Rule("1er quartile <= mediane", "revenu_q1 IS NOT NULL AND revenu_q1 > revenu_median"),
]

RISQUES_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("nb d'arretes catnat non negatif", "nb_arretes_catnat < 0", critical=True),
    Rule(
        "sous-total inondation <= total",
        "nb_arretes_inondation > nb_arretes_catnat",
        critical=True,
    ),
]

EQUIPEMENTS_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("nb de services & sante non negatif", "nb_services_sante < 0", critical=True),
    Rule("nb de loisirs & culture non negatif", "nb_loisirs_culture < 0", critical=True),
]

SECURITE_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("taux de delinquance non negatif", "taux_delinquance_global < 0", critical=True),
    Rule("taux dans la borne plausible", "taux_delinquance_global > 2000", critical=True),
    Rule("au moins un indicateur diffuse", "nb_indicateurs_diffuses < 1", critical=True),
]

EMPLOI_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("taux de chomage dans [0, 50] %", "taux_chomage < 0 OR taux_chomage > 50", critical=True),
    Rule(
        "taux de couverture emploi dans [0, 50]",
        "taux_couverture_emploi < 0 OR taux_couverture_emploi > 50",
        critical=True,
    ),
    Rule("population active presente", "pop_active IS NULL OR pop_active <= 0", critical=True),
]

CLIMAT_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule(
        "ensoleillement dans [500-3200 h/an]",
        "ensoleillement_h_an < 500 OR ensoleillement_h_an > 3200",
        critical=True,
    ),
    # Borne haute 500 (et non 365) : jours_ensoleilles = ensoleillement/6,5 est
    # une heuristique qui peut dépasser 365 par construction (3200 h/an -> 492).
    # La règle d'origine [60-365] était violée par les données de référence
    # elles-mêmes (2 614 communes, max 446) — elle n'était pas bloquante dans
    # le notebook, elle le devient ici (validate_silver échoue le run).
    Rule(
        "jours ensoleilles dans [60-500]",
        "jours_ensoleilles < 60 OR jours_ensoleilles > 500",
        critical=True,
    ),
    Rule(
        "coherence jours/heures (jours <= h/an <= jours x 16)",
        "ensoleillement_h_an < jours_ensoleilles "
        "OR ensoleillement_h_an > jours_ensoleilles * 16",
        critical=True,
    ),
    Rule(
        "temperature annuelle dans [-5, 25] degres",
        "temperature_moy_annuelle IS NOT NULL "
        "AND (temperature_moy_annuelle < -5 OR temperature_moy_annuelle > 25)",
    ),
    Rule("distance a la station raisonnable (< 150 km)", "dist_station_km > 150"),
]

TOURISME_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule(
        "part de residences secondaires dans [0,1]",
        "part_residences_secondaires < 0 OR part_residences_secondaires > 1",
        critical=True,
    ),
    Rule("nombre de logements strictement positif", "nb_logements <= 0", critical=True),
]

PROXIMITE_METROPOLE_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("distance strictement positive", "dist_metropole_km <= 0", critical=True),
    Rule(
        "distance dans les bornes plausibles (< 500 km)",
        "dist_metropole_km > 500",
        critical=True,
    ),
    Rule("metropole de reference presente", "nom_metropole IS NULL", critical=True),
]

DPE_RULES = [
    Rule(
        "etiquette dans A-G",
        "etiquette_dpe NOT IN ('A','B','C','D','E','F','G')",
        critical=True,
    ),
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
]

# Avis ville-ideale (produits par l'étape NLP externe). La couverture est
# volontairement partielle (quelques milliers de communes au mieux) : aucune
# règle de couverture ici, seulement la cohérence intrinsèque.
AVIS_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule(
        "date presente et plausible (>= 2005)",
        "date_avis IS NOT NULL AND date_avis < DATE '2005-01-01'",
        critical=True,
    ),
    Rule(
        "note moyenne dans [0, 10]",
        "note_moyenne IS NOT NULL AND (note_moyenne < 0 OR note_moyenne > 10)",
        critical=True,
    ),
]

AVIS_SEGMENTS_RULES = [
    Rule("code commune sur 5 caracteres", "length(code_commune) <> 5", critical=True),
    Rule("sentiment dans [-1, 1]", "sentiment < -1 OR sentiment > 1", critical=True),
    Rule(
        "champ de polarite attendu (positif/negatif)",
        "polarity_field NOT IN ('positif', 'negatif')",
        critical=True,
    ),
    Rule("texte de segment non vide", "length(trim(text)) < 15"),
]

# Association table silver -> règles, consommée par validate_silver.
SILVER_RULES: dict[str, list[Rule]] = {
    "dvf": DVF_RULES,
    "commune_transport": TRANSPORT_RULES,
    "revenus": REVENUS_RULES,
    "risques": RISQUES_RULES,
    "equipements": EQUIPEMENTS_RULES,
    "securite": SECURITE_RULES,
    "emploi": EMPLOI_RULES,
    "climat": CLIMAT_RULES,
    "tourisme": TOURISME_RULES,
    "proximite_metropole": PROXIMITE_METROPOLE_RULES,
    "dpe": DPE_RULES,
    "avis": AVIS_RULES,
    "avis_segments": AVIS_SEGMENTS_RULES,
}
