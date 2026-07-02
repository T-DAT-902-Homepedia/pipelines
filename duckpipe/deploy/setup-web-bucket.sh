#!/usr/bin/env bash
# Création one-shot du bucket public servant les artefacts web statiques à la
# webapp (cf. ADR-0013). À exécuter UNE FOIS par un administrateur du projet.
set -euo pipefail

PROJECT_ID="project-aab739fb-fb38-4996-84e"
BUCKET="gs://homepedia-web"
RUNNER_SA="pipeline-runner@${PROJECT_ID}.iam.gserviceaccount.com"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

gcloud storage buckets create "${BUCKET}" \
  --project "${PROJECT_ID}" --location=europe-west1 --uniform-bucket-level-access

# Lecture publique (la webapp fetch directement les objets) ; écriture
# réservée au service account du pipeline.
gcloud storage buckets add-iam-policy-binding "${BUCKET}" \
  --member=allUsers --role=roles/storage.objectViewer

gcloud storage buckets add-iam-policy-binding "${BUCKET}" \
  --member="serviceAccount:${RUNNER_SA}" --role=roles/storage.objectAdmin

gcloud storage buckets update "${BUCKET}" --cors-file="${SCRIPT_DIR}/web-cors.json"

echo "OK — https://storage.googleapis.com/${BUCKET#gs://}/v1/meta.json"
