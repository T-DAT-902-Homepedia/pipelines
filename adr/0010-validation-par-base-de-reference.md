---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Validation du portage par comparaison à la base de référence

## Contexte et problème

Le portage de `exploration/src/` vers `duckpipe` réécrit ~15 traitements
SQL. Comment prouver que la logique portée est strictement fidèle à
l'original, au-delà de tests unitaires sur données synthétiques ?

## Facteurs de décision

- La base `exploration/data/exploration.duckdb` (182 Mo) contient les tables
  produites par le code original sur les vraies données : c'est un oracle de
  non-régression gratuit.
- Les données brutes (~1,4 Go) sont déjà en cache local dans
  `exploration/data/raw/` — aucun téléchargement nécessaire.
- Une divergence de logique (seuil, filtre, jointure) se manifeste presque
  toujours par une volumétrie différente.

## Options envisagées

- Tests unitaires sur fixtures synthétiques uniquement
- Fixtures synthétiques + tests d'intégration sur données réelles comparés
  aux comptes exacts de la base de référence

## Décision

Option retenue : « les deux niveaux ». Les fixtures synthétiques valident le
comportement des abstractions ; des tests d'intégration (marqués `skipif` si
`exploration/data/raw/` est absent) rejouent chaque pipeline porté sur les
données réelles et assertent l'égalité **exacte** avec les comptes de la
base de référence (34 928 communes, 747 201 transactions DVF, 31 447
communes prix 2021, etc.). Cette exigence d'exactitude est ce qui a motivé
l'ADR-0008 (`threads = 1`).

### Conséquences

- Bon : la méthode a détecté deux bugs réels pendant le portage — un filtre
  de coordonnées manquant dans `prix_millesime` et le non-déterminisme de
  l'extension spatiale (ADR-0008).
- Bon : toute divergence future des seuils dupliqués (ADR-0007) casse les
  tests.
- Mauvais : suite plus lente (~2 min) et dépendante des données locales —
  ces tests sont automatiquement sautés en CI sans les données.
- Point de vigilance : si les traitements de `duckpipe` évoluent
  volontairement au-delà de la référence, les comptes attendus devront être
  re-établis et la référence gelée (snapshot versionné des volumétries).
