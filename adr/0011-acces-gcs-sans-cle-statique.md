---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Accès GCS sans clé statique (tunnel google-cloud-storage)

## Contexte et problème

L'ADR-0009 supposait que DuckDB s'authentifierait sur `gs://` via la
credential chain (metadata server). C'est faux : l'accès natif de DuckDB à
GCS passe par l'API S3-compatible de Google, qui n'accepte **que des clés
HMAC statiques** — ni les ADC locaux, ni le metadata server Cloud Run ne
servent sur ce chemin. La création d'une clé HMAC pour le service account
`pipeline-runner` a de plus été refusée par la politique d'organisation
(`constraints/iam.disableServiceAccountKeyCreation`).

## Facteurs de décision

- La politique d'organisation interdit les clés statiques de service account
  (et c'est une bonne pratique : pas de secret longue durée à gérer/faire
  tourner).
- Les volumes par fichier sont modestes (≤ ~600 Mo) : une copie temporaire
  locale est indolore.
- Les tests doivent rester exécutables sans credentials.

## Options envisagées

- Clé HMAC + Secret Manager (accès `gs://` natif DuckDB en streaming)
- Affaiblir la politique d'organisation pour autoriser la clé
- Tunnel : les Datasets transitent par un fichier temporaire local via
  google-cloud-storage (OAuth ADC en local, metadata server sur Cloud Run)

## Décision

Option retenue : « tunnel google-cloud-storage », implémentée par deux
context managers (`fetch.local_read_path` / `fetch.local_write_path`)
utilisés par tous les Datasets pour les chemins `gs://`. DuckDB ne parle
plus jamais directement à GCS ; l'extension `httpfs` n'est plus chargée.
`google-cloud-storage` reste une dépendance optionnelle (extra `gcs`)
installée dans l'image conteneur uniquement.

### Conséquences

- Bon : zéro credential statique ; en local ADC (`gcloud auth
  application-default login`), sur Cloud Run le metadata server — aucune
  configuration de secret nulle part.
- Bon : les tests locaux ne touchent jamais ce chemin (LSP conservé, cf.
  ADR-0005 : seule la racine des chemins change).
- Mauvais : une copie temporaire locale par fichier GCS lu/écrit (négligeable
  à notre échelle ; à revisiter si DuckDB gagne un jour un support OAuth
  natif pour GCS).
- Piège documenté : une variable d'environnement
  `GOOGLE_APPLICATION_CREDENTIALS` résiduelle écrase les ADC et détourne
  silencieusement les écritures vers un autre projet (vécu pendant la mise
  en place — vérifier son shell).
