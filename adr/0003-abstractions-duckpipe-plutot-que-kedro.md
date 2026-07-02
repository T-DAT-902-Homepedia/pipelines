---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Abstractions maison duckpipe plutôt que Kedro

## Contexte et problème

L'industrialisation demande une abstraction `Dataset` (persistance
`load`/`save` indépendante du format et du backend) et une abstraction
`Pipeline`/`Node` (transformations chaînables). Kedro fournit tout cela et
est déjà utilisé dans `dvf/` ; l'alternative est un cadre minimal maison.

## Facteurs de décision

- SOLID : OCP (nouveaux formats sans modifier le cœur), LSP (datasets
  substituables), DIP (les nodes ne dépendent que d'abstractions).
- KISS : ne pas réinventer ni embarquer un orchestrateur — l'ordonnancement
  inter-tâches est délégué à l'orchestrateur cloud (cf. ADR-0009).
- Kedro est pensé DataFrame + catalog YAML + runner propriétaire, alors que
  le moteur retenu est DuckDB-SQL (cf. ADR-0004).

## Options envisagées

- Kedro (comme la génération `dvf/`)
- Cadre maison minimal (`duckpipe`)
- Scripts procéduraux sans abstraction (comme `ingest/`)

## Décision

Option retenue : « cadre maison minimal », soit environ 300 lignes :
`Dataset` (ABC + implémentations par format), `Catalog` (registry nom
logique → Dataset, injecté aux nodes), `Node`/`Pipeline` (exécution
séquentielle explicite, composition par `+`), `pipeline_registry`
(point d'entrée unique pour l'orchestrateur). Volontairement **sans
résolution automatique de graphe de dépendances** : à l'intérieur d'une
tâche, 1 à 4 nodes dont l'ordre est trivial ; entre tâches, l'orchestrateur
fait ce travail.

### Conséquences

- Bon : surface de code minuscule, entièrement testée, aucune dépendance
  lourde ; changer de backend de stockage ne touche pas les nodes.
- Bon : un node est réutilisable tel quel entre test (fixtures locales) et
  prod (GCS) — seul le Catalog change.
- Mauvais : on perd l'outillage Kedro (kedro-viz, resume, versioning de
  datasets) — assumé, l'orchestrateur cloud couvre logs et reprises.
