"""Test d'intégration du runner NLP, hors-ligne (NullBackend, pas de torch).

Utilise le vrai spaCy fr_core_news_md (gardé par importorskip pour la CI
légère) mais aucun transformer : rapide et reproductible. Vérifie les schémas
Parquet exacts et l'anonymisation (aucun pseudo distinctif ne fuit).
"""

from pathlib import Path

import pytest

pytest.importorskip("spacy")
pytest.importorskip("pyarrow")

import pyarrow.parquet as pq  # noqa: E402

from homepedia_ville_ideale.nlp.runner import build_nlp_outputs  # noqa: E402
from homepedia_ville_ideale.nlp.sentiment import NullBackend  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "avis_mini.csv"


@pytest.fixture(scope="module")
def nlp():
    import spacy

    try:
        return spacy.load("fr_core_news_md")
    except OSError:
        pytest.skip("modèle fr_core_news_md non installé")


@pytest.fixture(scope="module")
def built(tmp_path_factory, nlp):
    out = tmp_path_factory.mktemp("silver")
    stats = build_nlp_outputs(FIXTURE, str(out), backend=NullBackend(), nlp=nlp)
    return out, stats


def _read(out: Path, rel: str):
    return pq.read_table(out / rel)


def test_stats_counts(built):
    _out, stats = built
    assert stats.n_avis == 4
    assert stats.n_communes == 3  # nice (x2), paris-1er, lyon-1er
    assert stats.model_name == "none"
    assert stats.n_segments > 0


def test_avis_schema_and_padding(built):
    out, _ = built
    table = _read(out, "avis_clean/avis.parquet")
    assert table.schema.names == [
        "avis_id", "code_commune", "slug", "nom_ville", "date_avis",
        "note_moyenne", "note_environnement", "sentiment_avis",
        "n_segments", "n_chars", "model_name", "nlp_version",
    ]
    codes = set(table.column("code_commune").to_pylist())
    assert "06088" in codes  # 6088 -> lpad 5
    assert all(len(c) == 5 for c in codes)


def test_missing_date_is_null(built):
    out, _ = built
    rows = _read(out, "avis_clean/avis.parquet").to_pylist()
    lyon = next(r for r in rows if r["slug"].startswith("lyon"))
    assert lyon["date_avis"] is None


def test_segments_schema_and_bounds(built):
    out, _ = built
    table = _read(out, "avis_nlp/segments.parquet")
    assert table.schema.names == [
        "avis_id", "code_commune", "date_avis", "segment_id", "polarity_field",
        "text", "themes", "model_score", "sentiment", "concordant",
    ]
    rows = table.to_pylist()
    assert all(-1.0 <= r["sentiment"] <= 1.0 for r in rows)
    assert all(r["polarity_field"] in ("positif", "negatif") for r in rows)
    assert all(r["model_score"] is None for r in rows)  # NullBackend


def test_themes_detected(built):
    out, _ = built
    rows = _read(out, "avis_nlp/segments.parquet").to_pylist()
    all_themes = {th for r in rows for th in (r["themes"] or [])}
    # le corpus mini touche plusieurs thèmes distincts
    assert {"transports", "commerces", "securite"} & all_themes


def test_tokens_schema_and_kinds(built):
    out, _ = built
    table = _read(out, "avis_nlp/tokens.parquet")
    assert table.schema.names == [
        "code_commune", "avis_id", "segment_id", "token", "kind", "themes", "sentiment",
    ]
    kinds = {r["kind"] for r in table.to_pylist()}
    assert kinds <= {"nom", "adj", "bigramme", "entite"}


def test_no_author_column_or_leak(built):
    out, _ = built
    # aucune colonne "auteur" dans aucune table
    for rel in ["avis_clean/avis.parquet", "avis_nlp/segments.parquet", "avis_nlp/tokens.parquet"]:
        assert "auteur" not in _read(out, rel).schema.names

    # aucun pseudo distinctif de la fixture ne fuit dans les textes/tokens
    distinctive = ["SecretPseudo42", "ZorroMasque", "AutreAuteurUnique", "AnonymeSansDate"]
    seg_text = " ".join(
        (r["text"] or "") for r in _read(out, "avis_nlp/segments.parquet").to_pylist()
    )
    tok_text = " ".join(
        (r["token"] or "") for r in _read(out, "avis_nlp/tokens.parquet").to_pylist()
    )
    for pseudo in distinctive:
        assert pseudo not in seg_text
        assert pseudo not in tok_text
