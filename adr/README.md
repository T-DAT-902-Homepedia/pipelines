# Architecture Decision Records (ADR)

Ce dossier trace les décisions d'architecture du projet pipelines Homepedia,
au format [MADR](https://adr.github.io/madr/) (Markdown Architecture Decision
Records).

## Conventions

- Un fichier par décision : `NNNN-titre-en-kebab-case.md`, numérotation
  croissante, jamais réutilisée.
- Statuts : `proposed`, `accepted`, `deprecated`, `superseded by ADR-NNNN`.
- Une décision acceptée ne se modifie plus : si elle change, on écrit un
  nouvel ADR qui la remplace et on passe l'ancienne en `superseded`.
- Nouveau record : copier [`template.md`](template.md).

## Index

| N° | Titre | Statut | Date |
|---|---|---|---|
| [0001](0001-duckdb-moteur-unique.md) | DuckDB comme moteur de données unique | accepted | 2026-07-02 |
| [0002](0002-stockage-gcs-medaillon-parquet.md) | Stockage GCS en médaillon bronze/silver/gold, format Parquet | accepted | 2026-07-02 |
| [0003](0003-abstractions-duckpipe-plutot-que-kedro.md) | Abstractions maison duckpipe plutôt que Kedro | accepted | 2026-07-02 |
| [0004](0004-nodes-sur-relations-duckdb.md) | Les nodes transforment des relations DuckDB, pas des DataFrames | accepted | 2026-07-02 |
| [0005](0005-dataset-unique-local-et-gcs.md) | Une seule implémentation Dataset pour local et GCS | accepted | 2026-07-02 |
| [0006](0006-connexion-duckdb-partagee-par-execution.md) | Une connexion DuckDB partagée par exécution | accepted | 2026-07-02 |
| [0007](0007-nettoyage-reimplemente-inline.md) | Règles de nettoyage réimplémentées inline, pas d'import cross-repo | accepted | 2026-07-02 |
| [0008](0008-threads-1-reproductibilite.md) | `threads = 1` pour la reproductibilité de l'extension spatiale | accepted | 2026-07-02 |
| [0009](0009-orchestration-cloud-run-jobs.md) | Orchestration Cloud Run Jobs + Workflows + Scheduler (remplace Cloud Composer) | accepted | 2026-07-02 |
| [0010](0010-validation-par-base-de-reference.md) | Validation du portage par comparaison à la base de référence | accepted | 2026-07-02 |
| [0011](0011-acces-gcs-sans-cle-statique.md) | Accès GCS sans clé statique (tunnel google-cloud-storage) | accepted | 2026-07-02 |
| [0012](0012-ci-cd-github-actions-wif.md) | CI/CD GitHub Actions avec Workload Identity Federation | accepted | 2026-07-02 |
| [0013](0013-serving-statique-cdn.md) | Serving statique : artefacts web pré-générés sur bucket public | accepted | 2026-07-02 |
| [0014](0014-consolidation-export-web.md) | Consolidation de l'export web : publish-web chemin unique | accepted | 2026-07-03 |
