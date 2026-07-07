"""Agrégation par commune de l'analyse d'avis (gold).

Consomme les trois tables silver produites par l'étape NLP externe
(``avis`` / ``avis_segments`` / ``avis_tokens``) et matérialise une ligne par
commune avec tout ce qu'exige l'écran « Analyse textuelle » de la maquette :
sentiment global, sentiment par thème, nuage de mots coloré, verbatims.

Choix de fidélité :
- ``sentiment_global`` : moyenne PAR AVIS puis moyenne des avis (1 avis = 1
  voix) — un avis long et bavard ne pèse pas plus qu'un avis bref.
- ``low_data`` : marqué sous MIN_AVIS_THEMES ; l'export web masque alors le
  sentiment par thème (non fiable statistiquement) mais garde le reste.
- tous les ``ORDER BY`` / ``LIMIT`` portent des départages complets (ADR-0008 :
  déterminisme entre exécutions).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from duckpipe.node import Node, Pipeline

if TYPE_CHECKING:
    import duckdb

# En dessous de ce nombre d'avis, l'analyse par thème n'est pas fiable :
# la commune est marquée low_data (masquage à l'export, cf. maquette).
MIN_AVIS_THEMES = 10
# Un thème n'est retenu pour une commune que s'il a assez de segments.
MIN_SEGMENTS_THEME = 3
# Seuils de classe de sentiment (positif / neutre / négatif).
SENT_POS = 0.15
SENT_NEG = -0.15
# Un verbatim est « nuancé » sous ce seuil d'intensité (ou si discordant).
VERBATIM_NUANCE = 0.25
# Bornes de longueur d'un verbatim affichable (ni fragment, ni pavé).
VERBATIM_MIN_CHARS = 80
VERBATIM_MAX_CHARS = 300
# Tailles maximales des collections exportées.
WORDCLOUD_MAX = 40
PREVIEW_MAX = 7
VERBATIM_POS = 3
VERBATIM_NEG = 3
VERBATIM_NUANCE_N = 2


def _sentiment_class(col: str) -> str:
    """Expression SQL : classe de couleur d'un sentiment moyen."""
    return (
        f"CASE WHEN {col} >= {SENT_POS} THEN 'positive' "
        f"WHEN {col} <= {SENT_NEG} THEN 'negative' ELSE 'neutral' END"
    )


def avis_commune(
    con: duckdb.DuckDBPyConnection,
    avis: str,
    avis_segments: str,
    avis_tokens: str,
) -> str:
    """Produit la table gold ``avis_commune`` (une ligne par commune)."""

    # 1) Méta commune : n_avis, période, sentiment_global (1 avis = 1 voix).
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _avis_meta AS
        WITH par_avis AS (
            SELECT code_commune, avis_id, avg(sentiment) AS s
            FROM {avis_segments}
            GROUP BY code_commune, avis_id
        )
        SELECT
            a.code_commune,
            any_value(a.nom_ville) AS nom_ville,
            count(DISTINCT a.avis_id) AS n_avis,
            min(a.date_avis) AS date_min,
            max(a.date_avis) AS date_max,
            round(avg(pa.s), 4) AS sentiment_global,
            count(DISTINCT a.avis_id) < {MIN_AVIS_THEMES} AS low_data
        FROM {avis} a
        LEFT JOIN par_avis pa USING (code_commune, avis_id)
        GROUP BY a.code_commune
        """
    )

    # 2) Sentiment par thème : une ligne (commune, thème) dépliée des listes.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _avis_themes AS
        WITH seg_theme AS (
            SELECT code_commune, unnest(themes) AS theme, sentiment
            FROM {avis_segments}
            WHERE themes IS NOT NULL AND len(themes) > 0
        ),
        agg AS (
            SELECT
                code_commune, theme,
                count(*) AS n_segments,
                round(avg(CAST(sentiment >= {SENT_POS} AS DOUBLE)), 3) AS pct_positive,
                round(avg(CAST(sentiment <= {SENT_NEG} AS DOUBLE)), 3) AS pct_negative,
                round(avg(sentiment), 3) AS score
            FROM seg_theme
            GROUP BY code_commune, theme
            HAVING count(*) >= {MIN_SEGMENTS_THEME}
        )
        SELECT
            code_commune,
            list({{'theme': theme, 'n_segments': n_segments,
                   'pct_positive': pct_positive, 'pct_negative': pct_negative,
                   'score': score}} ORDER BY n_segments DESC, theme) AS themes
        FROM agg
        GROUP BY code_commune
        """
    )

    # 3) Nuage de mots : poids = nb d'avis distincts contenant le token
    #    (dédup par avis), sentiment moyen -> classe de couleur, thèmes unis.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _avis_wordcloud AS
        WITH tok AS (
            SELECT
                code_commune, lower(token) AS token, any_value(token) AS display,
                any_value(kind) AS kind,
                count(DISTINCT avis_id) AS weight,
                avg(sentiment) AS sent,
                flatten(list(themes)) AS theme_bag
            FROM {avis_tokens}
            GROUP BY code_commune, lower(token)
        ),
        ranked AS (
            SELECT
                code_commune, display AS word, weight,
                {_sentiment_class("sent")} AS sentiment,
                list_distinct(theme_bag) AS themes,
                row_number() OVER (
                    PARTITION BY code_commune ORDER BY weight DESC, display
                ) AS rk
            FROM tok
        )
        SELECT
            code_commune,
            list({{'word': word, 'weight': weight, 'sentiment': sentiment,
                   'themes': themes}} ORDER BY weight DESC, word)
                FILTER (rk <= {WORDCLOUD_MAX}) AS wordcloud,
            list({{'word': word, 'weight': weight, 'sentiment': sentiment}}
                 ORDER BY weight DESC, word)
                FILTER (rk <= {PREVIEW_MAX}) AS wordcloud_preview
        FROM ranked
        GROUP BY code_commune
        """
    )

    # 4) Verbatims : segments affichables, max 1 par avis, labellisés, diversifiés.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE _avis_verbatims AS
        WITH candidates AS (
            SELECT
                code_commune, avis_id, date_avis, segment_id, text, themes, sentiment,
                CASE
                    WHEN concordant = false OR abs(sentiment) < {VERBATIM_NUANCE} THEN 'Nuancé'
                    WHEN sentiment >= {VERBATIM_NUANCE} THEN 'Positif'
                    ELSE 'Négatif'
                END AS label,
                coalesce(themes[1]::VARCHAR, 'autre') AS theme_key
            FROM {avis_segments}
            WHERE length(text) BETWEEN {VERBATIM_MIN_CHARS} AND {VERBATIM_MAX_CHARS}
        ),
        one_per_avis AS (
            SELECT * FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY code_commune, avis_id
                    ORDER BY abs(sentiment) DESC, segment_id
                ) AS rk_avis
                FROM candidates
            ) WHERE rk_avis = 1
        ),
        ranked AS (
            SELECT *,
                row_number() OVER (
                    PARTITION BY code_commune, label
                    ORDER BY abs(sentiment) DESC, date_avis DESC, avis_id
                ) AS rk_label
            FROM one_per_avis
        ),
        picked AS (
            SELECT * FROM ranked
            WHERE (label = 'Positif'  AND rk_label <= {VERBATIM_POS})
               OR (label = 'Négatif'  AND rk_label <= {VERBATIM_NEG})
               OR (label = 'Nuancé'   AND rk_label <= {VERBATIM_NUANCE_N})
        )
        SELECT
            code_commune,
            list({{'text': text, 'label': label,
                   'theme': theme_key, 'mois': strftime(date_avis, '%Y-%m'),
                   'source': 'Ville-idéale'}}
                 ORDER BY label, rk_label) AS verbatims
        FROM picked
        GROUP BY code_commune
        """
    )

    # 5) Assemblage final. Le nuage est toujours présent (n_avis > 0) ; les
    #    thèmes/verbatims peuvent être NULL (coalesce vers liste vide).
    con.execute(
        """
        CREATE OR REPLACE TABLE avis_commune AS
        SELECT
            m.code_commune,
            m.nom_ville,
            m.n_avis,
            m.date_min,
            m.date_max,
            m.sentiment_global,
            m.low_data,
            coalesce(t.themes, []) AS themes,
            coalesce(w.wordcloud, []) AS wordcloud,
            coalesce(w.wordcloud_preview, []) AS wordcloud_preview,
            coalesce(v.verbatims, []) AS verbatims
        FROM _avis_meta m
        LEFT JOIN _avis_themes t USING (code_commune)
        LEFT JOIN _avis_wordcloud w USING (code_commune)
        LEFT JOIN _avis_verbatims v USING (code_commune)
        ORDER BY m.code_commune
        """
    )
    return "avis_commune"


avis_pipeline = Pipeline(
    nodes=[
        Node(
            func=avis_commune,
            inputs=["avis", "avis_segments", "avis_tokens"],
            outputs=["avis_commune"],
            name="avis_commune",
        ),
    ]
)
