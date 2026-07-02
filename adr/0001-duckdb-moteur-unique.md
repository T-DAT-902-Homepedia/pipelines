---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# DuckDB comme moteur de données unique

## Contexte et problème

Il faut industrialiser les traitements de données du repo `exploration/`
(ingestion, nettoyage, jointures spatiales, agrégations, scoring communal).
Deux générations de code coexistent déjà dans `pipelines/` : `dvf/`
(Kedro + PySpark 3.5 + MinIO + PostGIS, nécessite une JVM et un cluster ou un
Spark local lourd) et `ingest/` (boto3/S3 procédural, sans transformation).
Le volume total des sources brutes est d'environ 1,3 Go, le résultat final
pèse 182 Mo, et le cycle de rafraîchissement est annuel.

## Facteurs de décision

- Le volume tient intégralement en RAM d'une machine modeste.
- `exploration/src/` — désigné comme référence d'implémentation — est déjà
  écrit à 100 % en SQL DuckDB (`CREATE OR REPLACE TABLE … AS SELECT`).
- Besoin d'opérations spatiales (lecture GeoJSON, point-in-polygon) et de
  lecture/écriture objet cloud sans couche d'intégration supplémentaire.
- Coût et complexité d'infrastructure à minimiser (projet à budget réel).

## Options envisagées

- Continuer sur PySpark + Kedro (génération `dvf/` existante)
- DuckDB seul
- pandas ou Polars

## Décision

Option retenue : « DuckDB seul », parce que le volume ne justifie aucun moteur
distribué, que le code de référence se porte quasi tel quel (même dialecte
SQL), et que les extensions natives `spatial` (`ST_Read`, `ST_Contains`,
`ST_Area_Spheroid`) et `httpfs` (`gs://`) couvrent tous les besoins sans
service externe.

### Conséquences

- Bon : portage fidèle et rapide depuis `exploration/src/`, zéro
  infrastructure de calcul à opérer, exécution locale et cloud identiques.
- Bon : les tests d'intégration tournent sur un poste de dev sans cluster.
- Mauvais : `dvf/` (Spark/Kedro) et `ingest/` (boto3) deviennent des
  générations obsolètes, conservées en l'état mais non maintenues.
- Mauvais : moteur mono-machine — à réévaluer si le volume change d'ordre de
  grandeur (×100), ce qui n'est pas la trajectoire du projet.
