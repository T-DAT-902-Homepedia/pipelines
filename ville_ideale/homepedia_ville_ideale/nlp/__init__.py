"""Analyse textuelle des avis ville-ideale.fr (sentiment, thèmes, nuage de mots).

Étape batch qui transforme le CSV d'avis scrapés (points_positifs /
points_negatifs) en trois tables silver Parquet consommées par le pipeline
duckpipe `avis` :

- ``avis_clean/avis.parquet``     — un avis par ligne (métadonnées + sentiment moyen)
- ``avis_nlp/segments.parquet``   — un segment de phrase par ligne (thèmes + sentiment)
- ``avis_nlp/tokens.parquet``     — un token de nuage de mots par ligne

Les dépendances lourdes (spaCy, transformers, torch) vivent ici, hors de
l'image duckpipe (cf. plan ADR-0013). Le modèle transformer est optionnel :
``--no-model`` retombe sur le seul signal structurel positif/négatif, ce qui
rend le package testable hors-ligne.
"""

NLP_VERSION = "1"
