---
status: accepted
date: 2026-07-09
decision-makers: équipe Homepedia
---

# Maille quartier : agrégats prix et gap qualité-prix à l'IRIS

## Contexte et problème

Le score global (`score_valeur`) et l'écart qualité-prix (`gap`,
`gap_pondere`) ne sont calculés qu'à la maille commune : à l'intérieur d'une
grande ville, la carte est aveugle à la variation entre quartiers — qui est
d'abord une variation de prix. Le DVF est pourtant géolocalisé au grain
mutation (lon/lat conservés en silver `dvf`), seule manque une maille
infra-communale de restitution.

## Facteurs de décision

- Seul le prix est disponible partout à une maille infra-communale (points
  DVF) ; sécurité, emploi et risques n'existent pas plus fin que la commune.
- Le seuil de fiabilité (>= 5 ventes) devient difficile à atteindre au grain
  quartier sur un seul millésime DVF.
- Déterminisme entre runs (ADR-0008) : pas de simplification de géométries
  calculée (ADR-0013), pas de tirage non reproductible.
- Contrat front : `schema_version` reste 1, ajouts additifs uniquement
  (ADR-0014) ; budget CDN surveillé (ADR-0013).

## Options envisagées

- **Maille IRIS INSEE** : découpage statistique officiel, contours nationaux
  publiés (CONTOURS-IRIS® IGN/INSEE, Licence Ouverte 2.0), quartiers nommés,
  et la plupart des sources INSEE (BPE, Filosofi, base logement) existent
  déclinées à l'IRIS — seule maille qui permette d'enrichir le score plus tard.
- Sections cadastrales (id_parcelle DVF) : écarté — aucune donnée socio-éco à
  cette maille, pas de noms, trop de zones sous le seuil de fiabilité.
- Carreaux INSEE 200 m/1 km : écarté — illisible produit (pas de notion de
  quartier), volumétrie carte élevée, pas de correspondance avec les autres
  sources.
- Score entièrement recalculé à l'IRIS d'emblée : écarté pour le MVP — exige
  4-5 nouvelles ingestions (BPE IRIS, Filosofi IRIS, re-fetch ADEME…) pour un
  signal dont l'essentiel (le prix) est déjà disponible. Prévu en phase 2.

## Décision

MVP « gap quartier » à la maille IRIS, qualité héritée de la commune :

- **Contours** : édition FlatGeoBuf 2026 de CONTOURS-IRIS® (Géoplateforme
  `data.geopf.fr`), déjà généralisée moyenne échelle — lue par `ST_Read`,
  aucune simplification calculée. Les IRIS PLM sont codés par arrondissement
  (751xx/6938x/132xx), même convention que le DVF : raccord par equi-join,
  et ne JAMAIS filtrer `iris_geom` sur `commune_geom` (Etalab ne connaît que
  75056).
- **Prix** : `iris_prix` poole le millésime courant (silver `dvf`) et les
  mutations géolocalisées des millésimes annexes (`dvf_points_<annee>`,
  seconde sortie de `prix_millesime`), médiane simple sans pondération de
  récence. Affectation point→IRIS contrainte à la commune de la mutation
  (equi-join puis `ST_Intersects`), point de frontière départagé au plus
  petit `code_iris`. Millésime annexe manquant = fenêtre réduite, pas d'échec.
- **Gold `score_quartier`** : `n_prix_iris` normalisé par le même `_norm` que
  le communal, sur la population retenue (IRIS fiables de communes scorées) ;
  `gap_iris = score_commune − n_prix_iris`,
  `gap_pondere_iris = gap_iris × n_access_fin_commune`.
- **Export web** : `choropleth/iris-high/{dept}.geojson`, communes multi-IRIS
  uniquement (les mono-IRIS dupliqueraient `communes-high` : ×5,5 d'économie
  CDN), clés meta additives `nb_iris`/`nb_iris_scores` pour le feature-gate
  front.

## Conséquences

- Mesuré sur le run local de validation : 16 409 IRIS exportés dont 14 491
  scorés, 104 fichiers départementaux (max 1,6 Mo brut), appariement
  DVF→IRIS 99,95 % (15 communes orphelines, décalage COG 2026 vs DVF —
  suivi par `iris_match` dans le rapport DQ silver).
- Le gap quartier d'une commune mono-IRIS mesure surtout l'effet de fenêtre
  (prix poolés vs millésime courant), pas un signal quartier — documenté,
  et ces IRIS ne sont pas exportés côté web.
- Phase 2 possible sans changement de schéma : recalcul à l'IRIS des
  dimensions qui le permettent (transport GPS, BPE `GEO_OBJECT='IRIS'`, DPE
  via champ IRIS de l'API ADEME, Filosofi IRIS avec flag secret statistique,
  base logement déjà à l'IRIS), les autres restant héritées.
- Repli si l'édition FlatGeoBuf disparaît : GPKG (archive .7z, extracteur à
  écrire) ou GeoParquet IGN (encodage GeoArrow non lu par duckdb-spatial à
  ce jour).
