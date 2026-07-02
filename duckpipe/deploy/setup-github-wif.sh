#!/usr/bin/env bash
# Configuration Workload Identity Federation pour le CD GitHub Actions.
#
# À exécuter UNE FOIS par un administrateur du projet. Autorise les workflows
# GitHub Actions du dépôt T-DAT-902-Homepedia/pipelines à impersoner le
# service account github-deployer, sans aucune clé statique (cf. ADR-0012).
set -euo pipefail

PROJECT_ID="project-aab739fb-fb38-4996-84e"
PROJECT_NUMBER="305656045089"
GITHUB_ORG="T-DAT-902-Homepedia"
GITHUB_REPO="${GITHUB_ORG}/pipelines"
DEPLOYER_SA="github-deployer@${PROJECT_ID}.iam.gserviceaccount.com"
RUNNER_SA="pipeline-runner@${PROJECT_ID}.iam.gserviceaccount.com"

# 1. Pool + provider OIDC GitHub (identités restreintes à l'organisation).
gcloud iam workload-identity-pools create github \
  --location=global --display-name="GitHub Actions" --project "${PROJECT_ID}"

gcloud iam workload-identity-pools providers create-oidc github-oidc \
  --location=global --workload-identity-pool=github \
  --display-name="GitHub OIDC" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
  --attribute-condition="assertion.repository_owner=='${GITHUB_ORG}'" \
  --project "${PROJECT_ID}"

# 2. Service account de déploiement, aux droits limités au strict nécessaire.
gcloud iam service-accounts create github-deployer \
  --display-name="GitHub Actions deployer" --project "${PROJECT_ID}"

for role in roles/artifactregistry.writer roles/run.developer roles/workflows.editor; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DEPLOYER_SA}" --role="${role}" --condition=None
done

# actAs sur pipeline-runner : requis pour mettre à jour le job Cloud Run et
# redéployer le workflow qui s'exécutent sous ce SA.
gcloud iam service-accounts add-iam-policy-binding "${RUNNER_SA}" \
  --member="serviceAccount:${DEPLOYER_SA}" \
  --role=roles/iam.serviceAccountUser --project "${PROJECT_ID}"

# 3. Seul CE dépôt GitHub peut impersoner github-deployer.
gcloud iam service-accounts add-iam-policy-binding "${DEPLOYER_SA}" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github/attribute.repository/${GITHUB_REPO}" \
  --role=roles/iam.workloadIdentityUser --project "${PROJECT_ID}"

echo "OK — provider: projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github/providers/github-oidc"
