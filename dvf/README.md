# dvf

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

This is your new Kedro project with PySpark setup, which was generated using `kedro 1.4.0`.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## Prérequis environnement (Spark)

La pipeline utilise **PySpark 3.5**, qui requiert **Java 17** (Java 21 n'est pas
officiellement supporté par Spark 3.5.x). La version Java est figée dans `.sdkmanrc`.

```bash
# Sélectionner Java 17 pour le projet (depuis pipelines/dvf/)
sdk env install   # installe la version listée dans .sdkmanrc si absente
sdk env           # active Java 17 dans le shell courant
```

> Astuce : activez `sdkman_auto_env=true` dans `~/.sdkman/etc/config` pour que
> `.sdkmanrc` soit appliqué automatiquement à l'entrée du dossier.

Spark s'appuie sur `JAVA_HOME` ; `sdk env` le positionne correctement. Vérification :

```bash
java -version            # doit afficher 17.x
python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[*]').getOrCreate(); print('OK')"
```

## Architecture médaillon (Bronze / Silver / Gold)

Le pipeline transforme le dataset Geo DVF 2025 en couches successives :

```
data/01_raw/geo_dvf.csv.gz
   │  pipeline ingestion (Spark, schéma explicite + typage date)
   ▼
MinIO  s3a://homepedia/bronze/geo_dvf/        BRONZE — Parquet, copie 1:1 typée
   │  pipeline silver (filtre qualité, dédoublonnage, prix/m²)
   ▼
MinIO  s3a://homepedia/silver/{mutations,biens_ppm2}/   SILVER — Parquet
   │  pipeline gold (agrégats + transactions, JDBC)
   ▼
PostGIS : agg_commune (~36k) · agg_departement (~194) · transactions (~731k)
```

Décisions : stockage objet **MinIO** (accès Spark `s3a://`, Parquet brut, pas
d'Iceberg vu le volume), couche Gold en **PostgreSQL/PostGIS**, Spark en
`local[*]` (bascule cluster/HDFS ultérieure). Méthodologie clé : `valeur_fonciere`
jamais sommée (constante par mutation), prix/m² sur mutations mono-bien bâti
uniquement, bornage des aberrations par `département × type_local`, médiane via
`percentile_approx`, communes à faible volume marquées `fiable=false` (non
supprimées).

### Démarrer l'infra données

Depuis la racine du dépôt (copier `.env.data.example` en `.env.data` au préalable) :

```bash
docker compose -f compose.data.yml --env-file .env.data up -d
```

Console MinIO : http://localhost:9001 · PostGIS : `localhost:5432`. Les accès
côté Kedro sont dans `conf/local/credentials.yml` (gitignoré) et doivent
correspondre à `.env.data`.

### Exécuter le pipeline

```bash
sdk env                       # Java 17 (cf. ci-dessous)
kedro run                     # bronze -> silver -> gold (end-to-end)
# ou par couche :
kedro run --pipeline ingestion
kedro run --pipeline silver
kedro run --pipeline gold
```

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention)
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Declare any dependencies in `requirements.txt` for `pip` installation.

To install them, run:

```
pip install -r requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

## How to test your Kedro project

Have a look at the files `tests/test_run.py` and `tests/pipelines/data_science/test_pipeline.py` for instructions on how to write your tests. Run the tests as follows:

```
pytest
```

You can configure the coverage threshold in your project's `pyproject.toml` file under the `[tool.coverage.report]` section.

## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. Install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html)
