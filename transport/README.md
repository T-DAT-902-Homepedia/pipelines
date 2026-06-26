# Pipeline transport — couche gold (silver parquet → PostGIS)

Charge les tables PostGIS servies par l'API transport (`/api/v1/transport/*`) à
partir de la couche **silver** (parquet, déjà nettoyée/typée) présente dans MinIO.

## Place dans le médaillon

```
data.gouv.fr CSV  ──ingest──▶  bronze/transport/*.csv
                                      │ (étape Spark : typage + nettoyage)
                                      ▼
                          silver/transport_stations   (= schéma transport_commune)
                          silver/transport_stops       (station + lon/lat)
                                      │ (ce pipeline : gold)
                                      ▼
                   PostGIS  transport_commune  /  transport_stops
```

- **Amont** : la couche silver est produite par l'étape Spark transport
  (bronze → silver). Les datasets `silver/transport_stations` et
  `silver/transport_stops` sont attendus dans le bucket S3.
- **Ce pipeline** ne fait que le **gold** : silver → PostGIS, avec dérivation de
  `geom` et `code_commune` pour les arrêts. Volume faible → Python pur (pyarrow +
  psycopg), pas de Spark.

## Schémas (source de vérité)

- `schemas/schemas/08_transport_commune.sql`
- `schemas/schemas/09_transport_stops.sql`

Appliquer les schémas avant le premier chargement si les tables n'existent pas.

## Exécution

```sh
cp .env.example .env   # adapter MinIO / PostGIS
set -a; source .env; set +a

uv run python scripts/load_transport_commune.py   # -> transport_commune
uv run python scripts/load_transport_stops.py     # -> transport_stops (geom + code_commune)
```

Les deux scripts sont **idempotents** (TRUNCATE + recharge). `load_transport_stops`
réalise le reverse-geocoding par jointure spatiale `ST_Contains` sur
`commune_geometry.geom_high` ; les arrêts hors territoire restent à
`code_commune = NULL`.
