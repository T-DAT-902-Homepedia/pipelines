---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# `threads = 1` pour la reproductibilité de l'extension spatiale

## Contexte et problème

Pendant la validation du portage sur données réelles, la chaîne
`ST_Read` (GeoJSON) + jointure spatiale `ST_Contains` a produit un nombre de
communes **non déterministe** d'un run à l'autre sur des données strictement
identiques : 34 927 ou 34 928 lignes selon l'exécution. Le phénomène
disparaît totalement en mono-thread (stable sur 5 runs consécutifs) et n'est
pas reproductible quand `threads = 1` est fixé dès la connexion — la source
de l'instabilité est l'exécution parallèle de l'extension spatiale de
DuckDB, pas notre SQL.

## Facteurs de décision

- Les tests de validation comparent des volumétries **exactes** à la base de
  référence : un résultat variable rend cette validation impossible.
- Le volume traité (~1,3 Go, cycle annuel) reste confortable en mono-thread.
- Un correctif localisé (threads=1 sur la seule requête de jointure) a été
  testé et s'est révélé insuffisant : l'instabilité vient déjà de `ST_Read`.

## Options envisagées

- Ignorer (tolérance ±1 ligne dans les tests)
- `threads = 1` localisé aux requêtes spatiales
- `threads = 1` global, fixé dans `get_connection()`

## Décision

Option retenue : « `threads = 1` global dans `get_connection()` ». La
reproductibilité bit-à-bit prime sur la vitesse d'exécution pour un pipeline
annuel. Le choix est documenté en commentaire dans `connection.py`.

### Conséquences

- Bon : résultats strictement reproductibles ; les tests d'égalité exacte
  des comptes restent possibles ; débogage sain.
- Mauvais : exécution plus lente (sans impact pratique : la suite complète
  de 25 tests sur données réelles tourne en ~2 min).
- À réévaluer si une version future de l'extension `spatial` DuckDB corrige
  le non-déterminisme sous parallélisme.
