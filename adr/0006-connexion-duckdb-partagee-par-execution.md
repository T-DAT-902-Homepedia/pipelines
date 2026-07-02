---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Une connexion DuckDB partagée par exécution

## Contexte et problème

Qui possède la connexion DuckDB : chaque node, chaque Dataset, ou
l'exécution (tâche d'orchestrateur / runner) ?

## Facteurs de décision

- DuckDB est single-writer : plusieurs connexions concurrentes sur une même
  base sont une source de verrous et de complexité.
- Les tables intermédiaires d'un pipeline doivent être visibles des nodes
  suivants sans persistance disque.
- L'isolation entre tâches existe déjà au niveau de l'orchestrateur (un
  process par tâche).

## Options envisagées

- Une connexion par node
- Une connexion stockée sur les Datasets
- Une connexion unique par exécution, passée en paramètre partout

## Décision

Option retenue : « une connexion unique par exécution », créée par
`get_connection()` (in-memory par défaut, extensions `spatial` + `httpfs`
chargées, secret GCS best-effort) et passée explicitement à tous les nodes
et datasets. C'est le pattern déjà en place dans `exploration/src/`. Ni les
nodes ni les Datasets ne stockent la connexion — couplage explicite,
testable avec une connexion `:memory:` par test.

### Conséquences

- Bon : simple, sans état caché, chaque tâche cloud ouvre/ferme sa propre
  connexion in-memory (le stockage durable est GCS, pas un fichier .duckdb).
- Mauvais : les tables d'une même exécution partagent un espace de noms —
  convention : le nom logique du Catalog EST le nom de table DuckDB.
