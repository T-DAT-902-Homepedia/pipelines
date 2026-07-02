---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Serving statique : artefacts web pré-générés sur bucket public

## Contexte et problème

Le pipeline produit bronze/silver/gold sur GCS (privé) mais rien ne sert ces
données à la webapp. L'API existante (`api/`, FastAPI + asyncpg) a été écrite
pour la génération PostGIS précédente, qui n'existe plus : la carte en prod
pointe une API sans base de données. Les données changent une fois par an ;
le budget est surveillé (alerte à 10 €/mois).

## Facteurs de décision

- Données figées entre deux runs annuels : aucune requête n'a besoin d'être
  calculée à la volée.
- Coût : une instance Cloud SQL 24 h/24 (~10-15 €/mois) dépasse le budget
  pour un besoin en lecture seule annuel.
- La webapp (deck.gl + MapLibre) consomme déjà des FeatureCollection GeoJSON
  avec un système maille×LOD — réutilisable tel quel en fichiers.
- L'org policy autorise les buckets publics (vérifié).

## Options envisagées

- Apache Iceberg au-dessus des Parquet : écarté — résout des problèmes
  absents ici (écrivains concurrents, schema evolution, time travel sur gros
  volumes) ; les partitions `run_date=` versionnent déjà.
- Cloud SQL PostgreSQL/PostGIS + étape publish-db (l'API actuelle
  fonctionnerait telle quelle)
- DuckDB embarqué dans l'API, données cuites dans l'image au publish
- Pré-génération statique de tous les artefacts web sur bucket public + CDN

## Décision

Option retenue : « serving 100 % statique ». Une étape `publish-web` du DAG
(commande CLI `python -m duckpipe publish-web`) génère et publie sur
`gs://homepedia-web` (public, CORS restreint aux origines de la webapp) :

- choroplèthes GeoJSON par maille×LOD, à partir des contours pré-simplifiés
  Etalab (50m/100m/1000m, topologie de couverture garantie par le
  producteur — `ST_CoverageSimplify` a été écarté après constat d'un
  non-déterminisme entre processus sur les données réelles, même famille de
  problèmes que l'ADR-0008), toutes les métriques dans les properties (les
  switchers type de local et métrique sont des re-colorisations sans
  refetch) ;
- LOD high communal découpé par département (la vue zoomée ne couvre que
  quelques départements) ;
- fiches communes groupées par département (prix, évolution par millésime,
  score et composantes, indicateurs silver — extensibles à la future
  perception NLP ville-ideale) ;
- index de recherche à clés courtes et classement des communes sous-cotées ;
- `meta.json` (seul objet muté, uploadé en dernier) pointant le run courant :
  swap atomique, artefacts immuables cachés un an, rollback en rééditant meta.

L'API `api/` est mise en pause : son design LOD/GeoJSON est repris sous forme
pré-générée. Elle redeviendra pertinente pour des besoins réellement
dynamiques (requêtes ad hoc, auth, écriture).

### Conséquences

- Bon : coût ~0 € (25 Mo stockés par run), latence CDN, zéro service à
  opérer, la webapp fonctionne même si tout le backend est éteint.
- Bon : chaque run est auto-versionné (`runs/{run_date}/`), testable en
  local par la même commande (`--env local`).
- Mauvais : toute nouvelle vue de données exige une re-publication (pas de
  requêtes à la volée) ; le travail asyncpg/PostGIS du repo `api/` est
  suspendu.
- Mauvais : bucket public — n'y publier que des données ouvertes (c'est le
  cas : tout provient de l'opendata).
