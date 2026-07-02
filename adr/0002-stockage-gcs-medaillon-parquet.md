---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Stockage GCS en médaillon bronze/silver/gold, format Parquet

## Contexte et problème

Le pipeline doit persister trois familles de données : les fichiers sources
bruts (CSV/JSON/GeoJSON hétérogènes), les tables nettoyées par source, et les
sorties finales (score communal + rapports de data quality) lues par l'API
FastAPI. La génération précédente (`dvf/`) écrivait sur MinIO auto-hébergé et
PostgreSQL/PostGIS.

## Facteurs de décision

- L'API doit lire le résultat sans qu'on opère une base de données.
- DuckDB lit et écrit nativement `gs://` via `httpfs` (cf. ADR-0001).
- Traçabilité : conserver le brut pour pouvoir rejouer un traitement.
- Simplicité d'exploitation et coût (~2-3 Go au total).

## Options envisagées

- PostgreSQL/PostGIS comme couche gold (génération `dvf/`)
- MinIO auto-hébergé
- Google Cloud Storage

## Décision

Option retenue : « GCS », organisé en architecture médaillon :

- `bronze/<source>/` : fichiers bruts tels que téléchargés, partitions
  Hive-style (`year=`, `dept=`) quand pertinent ;
- `silver/<table>/` : tables nettoyées par source, en Parquet (GeoParquet
  pour les géométries communales) ;
- `gold/score_territoire/run_date=<date>/` + copie `latest/` (chemin stable
  lu par l'API via `duckdb.read_parquet('gs://…')`), et `gold/dq_reports/`.

### Conséquences

- Bon : aucune base à opérer ; l'API lit un chemin stable ; chaque run est
  archivé par `run_date` et rejouable depuis le bronze.
- Bon : le même code lit un chemin local en test et `gs://` en prod.
- Mauvais : le type `GEOMETRY` DuckDB ne s'exporte pas tel quel en Parquet
  plat — les géométries transitent en GeoParquet ou restent des tables
  intermédiaires en mémoire (cf. `MemoryDataset`).
- Mauvais : nom de bucket global (`gs://homepedia-data` peut être pris) —
  repli prévu sur un nom suffixé par le numéro de projet.
