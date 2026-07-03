---
status: accepted
date: 2026-07-03
decision-makers: équipe Homepedia
---

# Consolidation de l'export web : publish-web chemin unique

## Contexte et problème

Deux chemins publiaient des artefacts sur le bucket public `gs://homepedia-web` :

- `webapp_export/export_score_geojson.py` : script standalone lancé à la main,
  sans test ni CI, produisant `v1/score.geojson` (une FeatureCollection
  commune par commune : prix, dpe, score, gap, gap_pondere et les 12
  dimensions normalisées), géométrie simplifiée à la volée par
  `ST_CoverageSimplify` — technique écartée par l'ADR-0013 pour
  non-déterminisme entre processus (cf. note dans `export_web.py`). La webapp
  consomme ce fichier directement.
- `duckpipe publish-web` (ADR-0013) : export intégré au DAG, testé, publiant
  des runs immuables `v1/runs/{run_date}/` + `meta.json` (swap atomique),
  géométries LOD Etalab pré-simplifiées. Sa choroplèthe communale n'exposait
  toutefois ni les dimensions du score, ni `dpe_dominant`, ni `gap`.

Deux producteurs pour le même bucket, deux contrats, deux techniques de
simplification : risque de divergence silencieuse et de données mortes (le
script manuel n'étant pas rejoué après les runs du DAG).

## Décision

`publish-web` devient l'unique chemin d'export/publication web :

- la choroplèthe communale expose désormais toutes les métriques du score
  (`gap`, `dpe_dominant`, les 12 dimensions `SCORE_DIMENSIONS`) — conforme à
  l'intention de l'ADR-0013 (re-colorisation sans refetch) ;
- `webapp_export/` est supprimé ;
- `publish-web` génère un artefact de compatibilité `v1/score.geojson`
  (builder `build_score_geojson_compat`) reprenant le contrat exact de
  l'ancien script : communes scorées uniquement, mêmes noms de properties,
  muté en place, cache court (`max-age=300`), uploadé hors du run immuable et
  avant `meta.json`. Il est DÉPRÉCIÉ dès sa création : il n'existe que pour
  laisser la webapp migrer sans rupture, désormais rafraîchi automatiquement
  à chaque run au lieu d'un lancement manuel.

La géométrie du compat vient du LOD Etalab 1000m (déterministe) et non plus
de `ST_CoverageSimplify` (~300 m) : rendu légèrement plus grossier à fort
zoom, assumé — la vue zoomée doit passer par `choropleth/communes-high/`.

### Condition de suppression du compat

Retirer `build_score_geojson_compat`, son appel, ses tests et l'objet GCS
`v1/score.geojson` quand la webapp consomme `meta.json` + `runs/` en prod,
confirmation de l'équipe front à l'appui. Mettre alors ce paragraphe à jour.

### Conséquences

- Bon : un seul producteur pour le bucket web, automatisé et testé ; le
  contrat historique reste servi et devient frais à chaque run.
- Bon : la carte peut se coloriser par n'importe quelle dimension du score
  depuis la choroplèthe (`communes-mid.geojson` mesuré à 30 Mo bruts,
  3,4 Mo gzippés servis).
- Mauvais : un artefact muté en place subsiste temporairement à la racine
  `v1/` (fenêtre de désynchronisation courte avec `meta.json` pendant la
  publication, acceptée pour un artefact déprécié).
