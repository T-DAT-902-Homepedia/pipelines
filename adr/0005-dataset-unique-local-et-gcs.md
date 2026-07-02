---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Une seule implémentation Dataset pour local et GCS

## Contexte et problème

Les datasets doivent lire des sources variées (CSV, CSV gzip, JSONL,
GeoJSON, membres de ZIP) et écrire du Parquet, en local pour les tests et
sur `gs://` en prod. Faut-il des classes par backend de stockage
(`LocalDataset`, `S3Dataset`, `GCSDataset`) comme l'intitulé initial le
suggérait ?

## Facteurs de décision

- DuckDB `httpfs` traite un chemin local et un chemin `gs://`/`s3://` de
  façon quasi identique : seul le scheme du chemin change.
- LSP : les tests doivent exercer la même implémentation que la prod, pas un
  double de test.
- Les tests ne doivent dépendre ni du réseau ni de credentials GCS.

## Options envisagées

- Une classe par backend (`GCSDataset`, `S3Dataset`, …) × par format
- Une classe par **format** (`CsvDataset`, `ParquetDataset`, `GeoJsonDataset`,
  `JsonDataset`), le backend étant porté par le chemin

## Décision

Option retenue : « une classe par format, chemin polymorphe ». La même
`ParquetDataset` sert en test (`tmp_path`) et en prod (`gs://…`) — LSP au
sens fort : même implémentation, pas seulement même interface. Les besoins
transverses se composent par décoration (`ZipMemberDataset` enveloppe un
Dataset interne) plutôt que par héritage (OCP). `MemoryDataset` couvre les
tables intermédiaires jamais persistées.

### Conséquences

- Bon : pas de duplication de classes par cloud provider ; suite de tests
  intégralement hors-ligne ; ajout d'un format = un fichier, zéro
  modification du cœur.
- Mauvais : `exists()` sur un chemin distant est best-effort (renvoie
  `False`, force le rechargement) — acceptable, l'idempotence est gérée au
  niveau orchestrateur.
