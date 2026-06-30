# Architecture — Pipeline Homepedia

## Contexte

Stack : **DuckDB** comme moteur unique (ingestion + spatial + scoring), **GCS** comme stockage objet, **Cloud Composer (Airflow)** comme orchestrateur. Cycle annuel, ~1,3 Go de sources brutes, 182 Mo de résultat.

Le code `exploration/src/` est la référence d'implémentation — les jobs sont des adaptations directes des fonctions existantes, avec GCS en entrée/sortie au lieu du système de fichiers local.

---

## DAG Airflow — `homepedia_pipeline`

```
@yearly  ─────────────────────────────────────────────────────────────────────────────────────────────

  ┌─────────────────────────────── GROUPE : ingest (parallèle) ──────────────────────────────────┐
  │                                                                                               │
  │  ingest_dvf          ingest_geometries   ingest_transport   ingest_climat                    │
  │  (DVF CSV.gz         (GeoJSON Etalab     (GTFS national     (GeoJSON stations                │
  │   → bronze/dvf/)      → bronze/geom/)    → bronze/          MF + parsing .data               │
  │                                           transport/)        → bronze/climat/)               │
  │                                                                                               │
  │  ┌──────────────────── SOUS-GROUPE : sources communales (parallèle) ──────────────────────┐  │
  │  │                                                                                         │  │
  │  │  ingest_emploi   ingest_securite   ingest_tourisme   ingest_bpe   ingest_revenus        │  │
  │  │  ingest_risques  ingest_dpe        ingest_proximite                                     │  │
  │  │  (CSV/ZIP → bronze/emploi/, bronze/securite/, …)                                        │  │
  │  └─────────────────────────────────────────────────────────────────────────────────────────┘  │
  └───────────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
  ┌─────────────────────────── GROUPE : preprocess (séquentiel partiel) ──────────────────────────┐
  │                                                                                                │
  │  preprocess_geometries                                                                         │
  │  (clean_geometries → silver/communes_geom/)                                                    │
  │         │                                                                                      │
  │         ▼                                                                                      │
  │  preprocess_dvf ──► preprocess_commune_agg                                                     │
  │  (clean_dvf         (agrégat médiane/p25/p75                                                   │
  │   → silver/          par commune                                                               │
  │   dvf_clean/)        → silver/commune_agg/)                                                    │
  │                                                                                                │
  │  preprocess_transport     ← dépend de preprocess_geometries (ST_Contains)                      │
  │  (clean_transport_stops + jointure spatiale → silver/transport_commune/)                        │
  │                                                                                                │
  │  preprocess_climat        ← dépend de preprocess_geometries (CROSS JOIN Haversine)             │
  │  (clean_climat → silver/climat_commune/)                                                       │
  │                                                                                                │
  │  preprocess_proximite     ← dépend de preprocess_geometries (CROSS JOIN Haversine)             │
  │  (clean_proximite_metropole → silver/proximite_commune/)                                       │
  │                                                                                                │
  │  ┌──── sources communales (parallèle, dépendent de preprocess_geometries) ────────────────┐   │
  │  │  preprocess_emploi   preprocess_securite   preprocess_tourisme                         │   │
  │  │  preprocess_bpe      preprocess_revenus    preprocess_risques   preprocess_dpe         │   │
  │  │  (clean_* → silver/<source>_commune/)                                                  │   │
  │  └────────────────────────────────────────────────────────────────────────────────────────┘   │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
  ┌──────────────────────────────── validate_silver ───────────────────────────────────────────────┐
  │  quality.validate() + quality.coverage() sur toutes les tables silver                          │
  │  → gold/dq_reports/silver_<run_date>.json                                                      │
  │  Échoue le DAG (task FAILED) si une règle critique est KO                                      │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
  ┌──────────────────────────────────── score ─────────────────────────────────────────────────────┐
  │  Fusionne les 9 tables silver (commune_agg + 8 dimensions)                                     │
  │  Normalisation _norm() p01–p99, composer_score(), gap, gap_pondere                             │
  │  → gold/score_territoire/run_date=YYYY/score.parquet                                           │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
  ┌──────────────────────────────── validate_gold ─────────────────────────────────────────────────┐
  │  Contrôles sur le gold : nb communes scorées, gap dans [-1, 1],                                │
  │  pas de commune avec score NULL, Top 25 stable vs run précédent (τ Kendall > 0.8)              │
  │  → gold/dq_reports/gold_<run_date>.json                                                        │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
  ┌──────────────────────────────── publish ───────────────────────────────────────────────────────┐
  │  Copie gold/score_territoire/run_date=YYYY/ → gold/score_territoire/latest/                    │
  │  (GCSToGCSOperator — l'API FastAPI lit toujours le chemin /latest/)                            │
  └────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Détail des tâches Airflow

| Tâche | Opérateur Airflow | Code source réutilisé | Sortie GCS |
|---|---|---|---|
| `ingest_dvf` | `PythonOperator` | `ingest.ensure_dvf` adapté | `bronze/dvf/year={{year}}/` |
| `ingest_geometries` | `PythonOperator` | `ingest.ensure_geometries` | `bronze/geom/` |
| `ingest_transport` | `PythonOperator` | `ingest.ensure_transport` (partie download) | `bronze/transport/` |
| `ingest_climat` | `PythonOperator` | `ingest_extra._build_stations_csv` | `bronze/climat/` |
| `ingest_<source>` (×7) | `PythonOperator` | `ingest_extra.ensure_*` (partie download) | `bronze/<source>/` |
| `preprocess_geometries` | `PythonOperator` | `preprocess.clean_geometries` | `silver/communes_geom/` |
| `preprocess_dvf` | `PythonOperator` | `preprocess.clean_dvf` | `silver/dvf_clean/` |
| `preprocess_commune_agg` | `PythonOperator` | `ingest.ensure_commune_agg` | `silver/commune_agg/` |
| `preprocess_transport` | `PythonOperator` | `preprocess.clean_transport_stops` + `ST_Contains` | `silver/transport_commune/` |
| `preprocess_climat` | `PythonOperator` | `preprocess.clean_climat` | `silver/climat_commune/` |
| `preprocess_proximite` | `PythonOperator` | `preprocess.clean_proximite_metropole` | `silver/proximite_commune/` |
| `preprocess_<source>` (×7) | `PythonOperator` | `preprocess.clean_*` | `silver/<source>_commune/` |
| `validate_silver` | `PythonOperator` | `quality.validate`, `quality.coverage` | `gold/dq_reports/` |
| `score` | `PythonOperator` | `section_score()` de `exploration/notebooks/exploration.py` | `gold/score_territoire/` |
| `validate_gold` | `PythonOperator` | `quality.validate` + règles gold custom | `gold/dq_reports/` |
| `publish` | `GCSToGCSOperator` | — | `gold/score_territoire/latest/` |

---

## Structure GCS

```
gs://homepedia-data/
├── bronze/
│   ├── dvf/year=2024/dept=75/  …  (partitionné par année et département)
│   ├── geom/communes.parquet
│   ├── transport/arrets.parquet
│   ├── climat/stations.parquet
│   ├── emploi/year=2021/
│   ├── securite/year=2022/
│   ├── tourisme/year=2021/
│   ├── bpe/year=2024/
│   ├── revenus/year=2021/
│   ├── risques/
│   └── dpe/
├── silver/
│   ├── communes_geom/           (GeoParquet)
│   ├── dvf_clean/year=2024/
│   ├── commune_agg/year=2024/
│   ├── transport_commune/
│   ├── climat_commune/
│   ├── proximite_commune/
│   ├── emploi_commune/
│   ├── securite_commune/
│   ├── tourisme_commune/
│   ├── equipements_commune/
│   ├── revenus_commune/
│   ├── risques_commune/
│   └── dpe_commune/
└── gold/
    ├── score_territoire/
    │   ├── run_date=2025-01-15/score.parquet
    │   └── latest/score.parquet   ← lu par l'API FastAPI
    └── dq_reports/
        ├── silver_2025-01-15.json
        └── gold_2025-01-15.json
```

---

## Décisions arrêtées

| Décision | Choix |
|---|---|
| Moteur | **DuckDB** seul (pas de Spark) — 1,3 Go tient en RAM, DuckDB natif sur GCS |
| Stockage | **GCS** `gs://homepedia-data/` |
| Orchestration | **Cloud Composer (Airflow)** — `@yearly` + trigger manuel |
| Format | **Parquet** (GeoParquet pour `communes_geom`) |
| API | Lit `gold/score_territoire/latest/` via `duckdb.read_parquet('gs://...')` |
| Ré-exécution partielle | Via `dag_run.conf = {"year": 2024}` sur les tâches DVF uniquement |
