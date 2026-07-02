---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# CI/CD GitHub Actions avec Workload Identity Federation

## Contexte et problème

Le packaging et le déploiement (image duckpipe, Cloud Run Job, DAG Workflows)
étaient manuels (`gcloud builds submit`, `gcloud run jobs update`,
`gcloud workflows deploy`). Il faut les automatiser à chaque évolution du
pipeline, et donc authentifier GitHub Actions auprès de GCP — sachant que la
politique d'organisation interdit les clés de service account
(`constraints/iam.disableServiceAccountKeyCreation`, cf. ADR-0011).

## Facteurs de décision

- Aucun secret statique possible (politique d'organisation) ni souhaitable
  (un secret GitHub qui fuite = accès au projet GCP).
- Les tests d'intégration sur données réelles se sautent automatiquement
  hors du poste de dev (skipif, ADR-0010) : la CI peut tourner sans données.
- Périmètre minimal : seul le dépôt `T-DAT-902-Homepedia/pipelines` doit
  pouvoir déployer.

## Options envisagées

- Clé JSON de service account en secret GitHub (interdit et déconseillé)
- Workload Identity Federation (OIDC GitHub -> impersonation d'un SA)
- Déploiement manuel conservé

## Décision

Option retenue : « Workload Identity Federation », en deux workflows :

- **CI** (`.github/workflows/ci.yml`) : ruff + pytest sur chaque PR et push
  main — suite hors-ligne uniquement, aucune authentification GCP.
- **CD** (`.github/workflows/deploy.yml`) : sur merge main touchant
  `duckpipe/` — build/push de l'image (taguée par SHA du commit), mise à
  jour du Cloud Run Job, redéploiement du DAG Workflows. Authentifié par
  OIDC : le provider n'accepte que les identités de l'organisation GitHub,
  et seul le dépôt `pipelines` peut impersoner le SA dédié
  `github-deployer` (droits limités : artifactregistry.writer,
  run.developer, workflows.editor, actAs pipeline-runner).

La configuration GCP (pool, provider, SA, bindings) est scriptée dans
`duckpipe/deploy/setup-github-wif.sh`, à exécuter une fois par un
administrateur.

### Conséquences

- Bon : zéro secret dans GitHub ; image traçable par SHA de commit ; le
  déploiement documenté = le déploiement exécuté.
- Bon : cohérent avec ADR-0011 — aucune clé statique nulle part dans le
  système.
- Mauvais : le premier setup WIF est verbeux (script fourni) ; le run annuel
  du DAG reste déclenché par Cloud Scheduler, indépendamment du CD.
