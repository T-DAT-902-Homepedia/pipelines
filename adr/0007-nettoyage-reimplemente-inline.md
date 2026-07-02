---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Règles de nettoyage réimplémentées inline, pas d'import cross-repo

## Contexte et problème

Les règles de nettoyage et seuils de plausibilité (prix/m², surfaces, bbox
France, bornes de revenu…) vivent dans `exploration/src/preprocess.py`.
`exploration/` et `pipelines/` sont deux dépôts git frères et indépendants :
comment `duckpipe` réutilise-t-il cette logique ?

## Facteurs de décision

- Un import par chemin relatif entre dépôts est fragile (suppose un clonage
  côte à côte, casse en CI et en conteneur).
- Publier un package partagé est prématuré pour deux consommateurs dont un
  (exploration) est un notebook jetable.
- Les seuils sont stables et documentés (constantes en tête de module).

## Options envisagées

- Import cross-repo par chemin relatif
- Extraction d'un package partagé publié
- Réimplémentation inline dans chaque module pipeline de `duckpipe`

## Décision

Option retenue : « réimplémentation inline ». Chaque module de
`duckpipe/pipelines/` redéclare les seuils qu'il utilise (ex. `PRIX_M2_MIN`,
`SURFACE_MAX`) avec un commentaire pointant la source. À revisiter si un
package partagé émerge (l'ADR sera alors remplacée).

### Conséquences

- Bon : dépôts totalement découplés, conteneur autonome, pas de dépendance
  d'installation exotique.
- Mauvais : duplication des seuils entre les deux repos — le risque de
  divergence est couvert par les tests de volumétrie contre la base de
  référence (cf. ADR-0010) : une divergence de seuil fait dévier les comptes
  et casse les tests. Ce filet a d'ailleurs détecté un filtre de coordonnées
  oublié lors du portage de `prix_millesime`.
