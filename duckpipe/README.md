# duckpipe

Abstractions `Dataset` et `Pipeline`/`Node` pour industrialiser les traitements de
`exploration/src/` sur la stack DuckDB + GCS décrite dans `pipelines/ARCHITECTURE.md`.

## Pourquoi les nodes manipulent des tables DuckDB, pas des `DataFrame`

`ARCHITECTURE.md` fixe DuckDB comme moteur unique : tout le calcul se fait en SQL
(`CREATE OR REPLACE TABLE ... AS SELECT`), sans DataFrame en mémoire. Une table/relation
DuckDB est l'équivalent strict d'un DataFrame dans ce moteur — un node
`DataFrame -> DataFrame` forcerait un aller-retour DuckDB <-> pandas à chaque étape
(RAM doublée, perte du lazy execution) pour aucun bénéfice sur un volume qui tient déjà
en RAM DuckDB (~1,3 Go de sources brutes).

Un `Node` a donc la signature :

```python
NodeFunc = Callable[..., dict[str, str] | str | None]
# func(con: DuckDBPyConnection, **inputs_resolus) -> outputs (nom(s) de table)
```

`Dataset` ne transporte pas les données entre nodes (DuckDB s'en charge via ses
tables) : il gère uniquement la **persistance aux frontières** du pipeline — lecture
des sources brutes hétérogènes (CSV, JSON...) et écriture/lecture Parquet
bronze/silver/gold, en local (tests) ou sur GCS (prod), via la même implémentation
(DuckDB `httpfs` traite un chemin local et un chemin `gs://` de façon quasi identique).

## Structure

- `datasets/` — implémentations de `Dataset` (`CsvDataset`, `ParquetDataset`, ...).
- `catalog.py` — `Catalog`, registry nom logique -> `Dataset`, injecté aux nodes.
- `node.py` — `Node`, `Pipeline` (exécution séquentielle, pas de résolution de
  graphe : Airflow orchestre déjà entre tâches).
- `pipeline_registry.py` — point d'entrée que les futures tâches Airflow
  importeront pour récupérer un pipeline nommé.
- `pipelines/` — pipelines concrets (ex. `dvf.py`, adaptation de
  `exploration/src/ingest.py`).

## Tests

```bash
uv run pytest
uv run ruff check .
```

Tous les tests tournent en local (DuckDB `:memory:`, fichiers `tmp_path`), sans
dépendance réseau ni credentials GCS. Les tests d'intégration sur données
réelles sont sautés automatiquement si `exploration/data/raw/` est absent.

## CLI

```bash
python -m duckpipe ingest <source|dvf|climat|dpe|all> [--year 2024] [--env local|prod]
python -m duckpipe run <pipeline|prix_millesime> [--year] [--env] [--run-date]
python -m duckpipe validate-silver [--env] [--run-date]
python -m duckpipe validate-gold [--env] [--run-date]
python -m duckpipe publish [--env] [--run-date]
```

`--env local --local-root <dir>` rejoue n'importe quelle étape sur un poste de
dev. En local, l'accès GCS utilise les ADC (`gcloud auth application-default
login`) ; attention à une variable `GOOGLE_APPLICATION_CREDENTIALS` résiduelle
dans le shell, qui les écraserait (cf. ADR-0011).

## Déploiement (Cloud Run Jobs + Workflows, cf. ADR-0009)

```bash
# Image (Cloud Build -> Artifact Registry)
gcloud builds submit --tag europe-west1-docker.pkg.dev/<projet>/homepedia/duckpipe:<version> .

# Job générique (les args sont surchargés par exécution par le workflow)
gcloud run jobs create duckpipe \
  --image europe-west1-docker.pkg.dev/<projet>/homepedia/duckpipe:<version> \
  --region europe-west1 --memory 8Gi --cpu 2 --task-timeout 3600 --max-retries 1 \
  --service-account pipeline-runner@<projet>.iam.gserviceaccount.com

# DAG
gcloud workflows deploy homepedia-pipeline --location europe-west1 \
  --source deploy/homepedia-pipeline.yaml \
  --service-account pipeline-runner@<projet>.iam.gserviceaccount.com

# Déclenchement annuel
gcloud scheduler jobs create http homepedia-pipeline-yearly \
  --location europe-west1 --schedule "0 3 1 2 *" \
  --uri "https://workflowexecutions.googleapis.com/v1/projects/<projet>/locations/europe-west1/workflows/homepedia-pipeline/executions" \
  --http-method POST --message-body '{}' \
  --oauth-service-account-email pipeline-runner@<projet>.iam.gserviceaccount.com
```
