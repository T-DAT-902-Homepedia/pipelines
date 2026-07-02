# Pipeline transport — couche gold (silver parquet duckpipe → PostGIS)

Charge la table PostGIS servie par l'API transport (`/api/v1/transport/*`) à
partir de la couche **silver** produite par **duckpipe** (`pipelines/duckpipe`).

## Place dans le médaillon

```
data.gouv.fr GTFS  ──duckpipe ingest──▶  bronze/transport/
                                              │ duckpipe run transport
                                              ▼
                              silver/transport_commune   (code_commune,
                              nb_arrets, densite_arrets_km2)
                                              │ (ce pipeline : gold)
                                              ▼
                                  PostGIS  transport_commune
```

- **Amont** : la couche silver est produite par **duckpipe** (source de vérité).
  Ce pipeline n'est qu'un **cache de service** : silver parquet → PostGIS, pour
  que l'API garde son accès asyncpg/PostGIS (comme le DVF), sans lecture GCS.
- Volume faible → Python pur (pyarrow + psycopg), pas de Spark ni de MinIO.

## Schéma (source de vérité)

- `schemas/schemas/08_transport_commune.sql`

Appliquer le schéma avant le premier chargement si la table n'existe pas.

## Exécution

```sh
# 1. produire le silver avec duckpipe (une fois) :
cd ../duckpipe
python -m duckpipe run geometries --env local
python -m duckpipe run transport  --env local

# 2. charger dans PostGIS :
cd ../transport
cp .env.example .env   # adapter HOMEPEDIA_SILVER_DIR / PostGIS
set -a; source .env; set +a
uv run python scripts/load_transport_commune.py   # -> transport_commune
```

Le script est **idempotent** (TRUNCATE + recharge).
