---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Les nodes transforment des relations DuckDB, pas des DataFrames

## Contexte et problème

L'intention initiale était des nodes « DataFrame → DataFrame » façon Kedro.
Or le code de référence (`exploration/src/`) et l'architecture cible sont
100 % SQL DuckDB : les fonctions manipulent des noms de table de bout en
bout, jamais un DataFrame en mémoire (pandas ne sert qu'à la visualisation
en aval).

## Facteurs de décision

- Un aller-retour DuckDB → pandas → DuckDB à chaque node doublerait la
  consommation mémoire et casserait l'exécution lazy du moteur.
- La relation/table DuckDB est l'équivalent strict du DataFrame dans ce
  moteur : la demande initiale est satisfaite sémantiquement.
- Fidélité au code de référence = portage direct, moins d'écarts à valider.

## Options envisagées

- Nodes `pandas.DataFrame → pandas.DataFrame`
- Nodes sur noms de relations DuckDB (`str → str`)

## Décision

Option retenue : « relations DuckDB ». La signature d'un node est
`func(con, **tables_entrée) -> nom(s) de table produite(s)` ; le `Dataset` ne
transporte pas les données entre nodes (DuckDB s'en charge via ses tables),
il gère uniquement la persistance aux frontières du pipeline (lecture des
bruts hétérogènes, écriture Parquet silver/gold).

### Conséquences

- Bon : aucune copie mémoire inutile ; SQL portable tel quel depuis
  `exploration/src/` ; testable avec une connexion `:memory:` et des
  fixtures de quelques lignes.
- Mauvais : signature moins familière qu'un DataFrame — documentée dans le
  README du package pour lever l'ambiguïté.
