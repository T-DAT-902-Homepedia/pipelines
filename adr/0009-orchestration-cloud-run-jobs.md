---
status: accepted
date: 2026-07-02
decision-makers: équipe Homepedia
---

# Orchestration Cloud Run Jobs + Workflows + Scheduler (remplace Cloud Composer)

## Contexte et problème

`ARCHITECTURE.md` prévoyait Cloud Composer (Airflow managé) avec des
`PythonOperator`. Or un environnement Composer tourne 24 h/24 et coûte
environ 300 à 400 €/mois — pour un DAG déclenché **une fois par an**
(`@yearly`), sur un projet au billing réel. Cette décision remplace la ligne
« Orchestration : Cloud Composer » de la table des décisions
d'`ARCHITECTURE.md`.

## Facteurs de décision

- Coût : ~4 000 €/an (Composer permanent) vs < 1 €/an (Cloud Run Jobs, forfait
  gratuit Workflows/Scheduler) pour la même charge.
- Le DAG est simple et figé (~17 tâches, dépendances connues) : la richesse
  d'Airflow (backfills, sensors, UI) n'apporte rien à un cycle annuel.
- `duckpipe` est un package Python autonome : une image conteneur légère
  suffit.

## Options envisagées

- Cloud Composer permanent (fidèle à ARCHITECTURE.md)
- Cloud Composer éphémère (créé pour le run annuel, détruit ensuite)
- Cloud Run Jobs + Cloud Workflows + Cloud Scheduler

## Décision

Option retenue : « Cloud Run Jobs + Workflows + Scheduler » :

- **une seule image Docker** `duckpipe` (python 3.13-slim, wheel du package,
  extensions DuckDB `spatial`/`httpfs` pré-installées au build), entrypoint
  CLI `python -m duckpipe run <pipeline> --year <YYYY>` ;
- **un seul Cloud Run Job** générique, les arguments étant surchargés par
  exécution (API `jobs.run` avec overrides) — pas quinze jobs à maintenir ;
- **Cloud Workflows** encode le DAG (branches `ingest_*` parallèles,
  dépendance sur `geometries` pour les preprocess spatiaux, puis
  validate_silver → score → validate_gold → publish) ;
- **Cloud Scheduler** déclenche le workflow annuellement, trigger manuel
  possible ;
- exécution sous un service account dédié `pipeline-runner` (droits
  `storage.objectAdmin` sur le bucket), sans fichier de clé — le détail du
  mécanisme d'authentification GCS est précisé (et corrigé) par
  [ADR-0011](0011-acces-gcs-sans-cle-statique.md) : DuckDB n'accède pas à
  GCS nativement, les Datasets tunnellent via google-cloud-storage.

La conteneurisation, écartée dans l'hypothèse Composer/PythonOperator,
devient le vecteur de déploiement — un seul artefact versionné.

### Conséquences

- Bon : coût quasi nul, aucun service permanent, image = artefact
  reproductible, montée en RAM ajustable par job (2-4 Gio pour le DVF).
- Mauvais : pas d'UI Airflow — observabilité via Cloud Logging et l'écran
  d'exécution Workflows ; le DAG en YAML Workflows est moins expressif qu'un
  DAG Python.
- Mauvais : écart avec `ARCHITECTURE.md`, qui doit être mis à jour pour
  pointer vers cet ADR.
