# ville_ideale — avis & analyse textuelle

Scraping des avis d'habitants de [ville-ideale.fr](https://www.ville-ideale.fr)
puis analyse de sentiment / thèmes / nuage de mots, pour l'écran « Analyse
textuelle » de la webapp.

Deux poids lourds de dépendances (spaCy, torch, transformers) vivent ici via
l'extra `nlp`, **hors** de l'image duckpipe (cf. `adr/0013`). L'étape NLP produit
des Parquet silver sur GCS ; duckpipe les agrège en gold (`pipelines/avis.py`).

## Flux

```
scraping  ──►  data/avis_france.csv  ──►  NLP build  ──►  silver Parquet (GCS)
(curl_cffi)                              (spaCy+CamemBERT)   avis / segments / tokens
                                                                   │
                                              duckpipe run avis ◄──┘  ──►  gold avis_commune
                                                                              │
                                                          duckpipe publish-web ──► avis/{dept}.json
```

## 1. Scraping (`scripts/scrape_ville_ideale.py`)

curl_cffi reproduit l'empreinte TLS/HTTP2 d'un Chrome réel (contourne JA4H, là
où requests/BeautifulSoup se font bloquer). Rotation d'IP pour tenir le débit
national — trois options par coût croissant :

1. **curl_cffi seul** (petit débit, local).
2. **AWS API Gateway** (`--aws-rotate`, extra `rotate`) : ~1 M req gratuites par
   région AWS.
3. **GitHub Actions matrix** (`.github/workflows/scrape-ville-ideale.yml`) :
   20 runners = 20 IP Azure, chacun scrape une tranche, un job final concatène.

Politesse intégrée : délai gaussien 3–5 s, backoff, détection de ban (8 communes
vides d'affilée → arrêt). Le scraping est **manuel** (ponctuel), pas une étape du
run annuel.

Entrée : un CSV `slug,code_commune` — `data/notes_par_commune.csv` (3095
communes, déjà commité) fait l'affaire ; les slugs suffixés du code y sont
gérés. Sortie : `data/avis_france.csv` (schéma identique à `data/avis_top80.csv`).

```bash
uv sync
uv run python scripts/scrape_ville_ideale.py \
    --communes data/notes_par_commune.csv --output data/avis_france.csv --max-pages 15
```

`data/avis_top80.csv` (78 grandes villes) est le corpus de référence commité,
suffisant pour développer/tester le pipeline aval.

## 2. Analyse NLP (`homepedia_ville_ideale.nlp`)

Sentiment hybride : polarité structurelle (`points_positifs`=+1 /
`points_negatifs`=-1) affinée par `cmarkea/distilcamembert-base-sentiment`
(5 étoiles → [-1, +1]). Thèmes par lexique (`PhraseMatcher` spaCy). Nuage de mots
= noms/adjectifs/entités-lieux. L'auteur n'est jamais écrit (anonymisation).

```bash
uv sync --extra nlp --extra gcs
# 3 Parquet silver (local ou gs://)
uv run python -m homepedia_ville_ideale.nlp build \
    --csv data/avis_top80.csv --silver-root ../duckpipe/data/silver
# corrélation sentiment vs note (métrique de validation, cible r >= 0.4)
uv run python -m homepedia_ville_ideale.nlp calibrate --csv data/avis_top80.csv
# hors-ligne (sans transformer) : signal structurel seul
uv run python -m homepedia_ville_ideale.nlp build ... --no-model
```

## Tests

```bash
uv sync --group dev        # base + dev : tests purs (segmenter, sentiment, parsing)
uv run pytest -q           # les tests spaCy/pyarrow s'auto-skippent sans l'extra nlp
```

## Notes

- `scripts/ingest_ville_ideale.py` : ingestion des **notes** agrégées (sans
  texte), flux distinct (S3), inchangé.
- Modèle et révision HF épinglés dans `nlp/sentiment.py` (reproductibilité).
