"""Orchestration : CSV d'avis → trois tables silver Parquet.

Lit le CSV scrapé, nettoie (code commune sur 5, dates, id d'avis anonyme),
segmente, annote via spaCy (thèmes + tokens), score le sentiment (backend),
puis écrit trois Parquet (avis / segments / tokens) en local ou sur GCS.

L'auteur d'un avis n'est utilisé QUE pour dériver ``avis_id`` (hash) et n'est
jamais écrit — anonymisation exigée par la maquette (« extraits anonymisés »).
"""

from __future__ import annotations

import csv
import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from homepedia_ville_ideale.nlp import NLP_VERSION
from homepedia_ville_ideale.nlp import extract as extract_mod
from homepedia_ville_ideale.nlp import themes as themes_mod
from homepedia_ville_ideale.nlp.segmenter import segment_review
from homepedia_ville_ideale.nlp.sentiment import MODEL_NAME, SentimentBackend, blend

if TYPE_CHECKING:
    from spacy.language import Language

logger = logging.getLogger(__name__)

# Champs CSV requis (schéma avis_top80.csv / avis_france.csv).
CSV_DATE_FORMAT = "%d-%m-%Y"


@dataclass
class BuildStats:
    n_avis: int
    n_segments: int
    n_tokens: int
    n_communes: int
    model_name: str


def _parse_date(raw: str) -> str | None:
    """``DD-MM-YYYY`` → ISO ``YYYY-MM-DD`` ; None si invalide."""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, CSV_DATE_FORMAT).date().isoformat()
    except ValueError:
        return None


def _to_float(raw: str) -> float | None:
    raw = (raw or "").strip().replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _avis_id(slug: str, date_raw: str, auteur: str, order: int) -> str:
    """Identifiant stable et anonyme d'un avis (l'auteur ne sort jamais d'ici).

    ``order`` (index de ligne du CSV) désambiguïse deux avis d'un même auteur à
    la même date sur la même commune.
    """
    payload = f"{slug}|{date_raw}|{auteur}|{order}".encode()
    return hashlib.sha1(payload).hexdigest()[:16]  # noqa: S324 — id non cryptographique


def _load_spacy() -> Language:
    import spacy  # noqa: PLC0415 — dépendance optionnelle "nlp"

    # parser + NER actifs (noun_chunks + entités), cf. extract.py.
    return spacy.load("fr_core_news_md")


# Schémas Parquet explicites (types stables indépendamment du contenu, ce qui
# garantit le round-trip DuckDB même quand une colonne est entièrement nulle).
_AVIS_SCHEMA = pa.schema([
    ("avis_id", pa.string()),
    ("code_commune", pa.string()),
    ("slug", pa.string()),
    ("nom_ville", pa.string()),
    ("date_avis", pa.date32()),
    ("note_moyenne", pa.float64()),
    ("note_environnement", pa.float64()),
    ("sentiment_avis", pa.float64()),
    ("n_segments", pa.int32()),
    ("n_chars", pa.int32()),
    ("model_name", pa.string()),
    ("nlp_version", pa.string()),
])

_SEGMENTS_SCHEMA = pa.schema([
    ("avis_id", pa.string()),
    ("code_commune", pa.string()),
    ("date_avis", pa.date32()),
    ("segment_id", pa.int32()),
    ("polarity_field", pa.string()),
    ("text", pa.string()),
    ("themes", pa.list_(pa.string())),
    ("model_score", pa.float64()),
    ("sentiment", pa.float64()),
    ("concordant", pa.bool_()),
])

_TOKENS_SCHEMA = pa.schema([
    ("code_commune", pa.string()),
    ("avis_id", pa.string()),
    ("segment_id", pa.int32()),
    ("token", pa.string()),
    ("kind", pa.string()),
    ("themes", pa.list_(pa.string())),
    ("sentiment", pa.float64()),
])


def _iso_to_date(iso: str | None):
    from datetime import date  # noqa: PLC0415

    return date.fromisoformat(iso) if iso else None


def build_nlp_outputs(
    csv_path: Path,
    silver_root: str,
    *,
    backend: SentimentBackend,
    nlp: Language | None = None,
) -> BuildStats:
    """Construit et écrit les trois tables silver depuis le CSV d'avis.

    ``silver_root`` peut être un dossier local ou un préfixe ``gs://`` : dans
    les deux cas on écrit d'abord des Parquet locaux, uploadés si distant.
    """
    if nlp is None:
        nlp = _load_spacy()
    matcher = themes_mod.build_matcher(nlp)

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    logger.info("%d avis lus depuis %s", len(rows), csv_path)

    # 1) Segmentation de tous les avis (indépendante du modèle).
    avis_records: list[dict] = []
    seg_units: list[dict] = []  # un par segment, avant annotation/scoring
    for order, row in enumerate(rows):
        code = (row["code_commune"] or "").strip().zfill(5)
        date_iso = _parse_date(row["date"])
        avis_id = _avis_id(row["slug"], row["date"], row["auteur"], order)
        segments = segment_review(row.get("points_positifs", ""), row.get("points_negatifs", ""))
        n_chars = len(row.get("points_positifs", "")) + len(row.get("points_negatifs", ""))
        avis_records.append({
            "avis_id": avis_id,
            "code_commune": code,
            "slug": row["slug"],
            "nom_ville": row["nom_ville"],
            "date_avis": date_iso,
            "note_moyenne": _to_float(row.get("note_moyenne", "")),
            "note_environnement": _to_float(row.get("environnement", "")),
            "n_chars": n_chars,
        })
        for seg in segments:
            seg_units.append({
                "avis_id": avis_id,
                "code_commune": code,
                "date_avis": date_iso,
                "polarity_field": seg.polarity_field,
                "order": seg.order,
                "text": seg.text,
            })

    # 2) Annotation spaCy (thèmes + tokens) en un seul passage nlp.pipe.
    texts = [u["text"] for u in seg_units]
    docs = list(nlp.pipe(texts)) if texts else []

    # 3) Scoring modèle par lot.
    model_scores = backend.score_batch(texts)

    # 4) Assemblage des lignes segments + tokens.
    seg_rows: list[dict] = []
    token_rows: list[dict] = []
    sentiment_by_avis: dict[str, list[float]] = {}
    # segment_id est l'ordre du segment dans son avis (0-based, stable).
    seg_counter: dict[str, int] = {}
    for unit, doc, model_score in zip(seg_units, docs, model_scores, strict=True):
        prior = 1 if unit["polarity_field"] == "positif" else -1
        sentiment = blend(prior, model_score)
        concordant = None if model_score is None else ((model_score >= 0) == (prior >= 0))
        seg_themes = themes_mod.tag_themes(doc, matcher)
        avis_id = unit["avis_id"]
        segment_id = seg_counter.get(avis_id, 0)
        seg_counter[avis_id] = segment_id + 1

        seg_rows.append({
            "avis_id": avis_id,
            "code_commune": unit["code_commune"],
            "date_avis": unit["date_avis"],
            "segment_id": segment_id,
            "polarity_field": unit["polarity_field"],
            "text": unit["text"],
            "themes": seg_themes,
            "model_score": model_score,
            "sentiment": sentiment,
            "concordant": concordant,
        })
        sentiment_by_avis.setdefault(avis_id, []).append(sentiment)

        for tok in extract_mod.extract_tokens(doc):
            token_rows.append({
                "code_commune": unit["code_commune"],
                "avis_id": avis_id,
                "segment_id": segment_id,
                "token": tok.token,
                "kind": tok.kind,
                "themes": seg_themes,
                "sentiment": sentiment,
            })

    # 5) Complète les avis avec sentiment moyen et nombre de segments.
    for rec in avis_records:
        seg_sent = sentiment_by_avis.get(rec["avis_id"], [])
        rec["sentiment_avis"] = sum(seg_sent) / len(seg_sent) if seg_sent else None
        rec["n_segments"] = len(seg_sent)
        rec["model_name"] = MODEL_NAME if any(s is not None for s in model_scores) else "none"
        rec["nlp_version"] = NLP_VERSION

    # 6) Écriture Parquet.
    _write_parquet(avis_records, _AVIS_SCHEMA, silver_root, "avis_clean/avis.parquet", date_cols={"date_avis"})
    _write_parquet(seg_rows, _SEGMENTS_SCHEMA, silver_root, "avis_nlp/segments.parquet", date_cols={"date_avis"})
    _write_parquet(token_rows, _TOKENS_SCHEMA, silver_root, "avis_nlp/tokens.parquet", date_cols=set())

    return BuildStats(
        n_avis=len(avis_records),
        n_segments=len(seg_rows),
        n_tokens=len(token_rows),
        n_communes=len({r["code_commune"] for r in avis_records}),
        model_name=MODEL_NAME if any(s is not None for s in model_scores) else "none",
    )


def _write_parquet(
    records: list[dict], schema: pa.Schema, silver_root: str, rel_path: str, *, date_cols: set[str]
) -> None:
    """Écrit une table Parquet (local ou GCS) depuis une liste de dicts."""
    columns: dict[str, list] = {name: [] for name in schema.names}
    for rec in records:
        for name in schema.names:
            value = rec.get(name)
            if name in date_cols:
                value = _iso_to_date(value)
            columns[name].append(value)
    table = pa.table(columns, schema=schema)

    if silver_root.startswith("gs://"):
        _write_parquet_gcs(table, f"{silver_root.rstrip('/')}/{rel_path}")
    else:
        dest = Path(silver_root) / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dest)
    logger.info("→ %d lignes : %s", table.num_rows, rel_path)


def _write_parquet_gcs(table: pa.Table, gcs_uri: str) -> None:
    import io  # noqa: PLC0415

    from google.cloud import storage  # noqa: PLC0415 — extra "gcs"

    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    bucket_name, _, blob_path = gcs_uri.removeprefix("gs://").partition("/")
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_path).upload_from_file(
        buffer, content_type="application/octet-stream"
    )


def calibrate(
    csv_path: Path, *, backend: SentimentBackend, nlp: Language | None = None
) -> dict:
    """Corrélation entre le sentiment texte agrégé par avis et ``note_moyenne``.

    Métrique de validation (pas d'affichage) : un sentiment fiable doit corréler
    avec la note auto-déclarée. Renvoie Pearson/Spearman + tailles.
    """
    if nlp is None:
        nlp = _load_spacy()

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    pred: list[float] = []
    ref: list[float] = []  # (note_moyenne - 5) / 5 → [-1, 1]
    for row in rows:
        note = _to_float(row.get("note_moyenne", ""))
        if note is None:
            continue
        segments = segment_review(row.get("points_positifs", ""), row.get("points_negatifs", ""))
        if not segments:
            continue
        texts = [s.text for s in segments]
        scores = backend.score_batch(texts)
        priors = [1 if s.polarity_field == "positif" else -1 for s in segments]
        sent = [blend(p, m) for p, m in zip(priors, scores, strict=True)]
        pred.append(sum(sent) / len(sent))
        ref.append((note - 5.0) / 5.0)

    return {
        "n": len(pred),
        "pearson": _pearson(pred, ref),
        "spearman": _spearman(pred, ref),
    }


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    n = len(xs)
    if n < 2:  # noqa: PLR2004
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    denom = (vx * vy) ** 0.5
    return round(cov / denom, 4) if denom else None


def _rank(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg_rank
        i = j + 1
    return ranks


def _spearman(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    if len(xs) < 2:  # noqa: PLR2004
        return None
    return _pearson(_rank(xs), _rank(ys))
