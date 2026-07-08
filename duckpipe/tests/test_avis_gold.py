"""Tests hors-ligne de l'agrégation gold des avis sur fixtures synthétiques.

Vérifie les formules exactes (sentiment global 1-avis-1-voix, dédup du nuage
par avis, labels de verbatims, flag low_data, seuil de segments par thème).
"""

from __future__ import annotations

import pytest

from duckpipe.pipelines.avis import (
    MIN_AVIS_THEMES,
    MIN_SEGMENTS_THEME,
    avis_commune,
)


def _seed(con, avis_rows: str, seg_rows: str, tok_rows: str) -> None:
    con.execute(
        f"CREATE TABLE avis AS SELECT * FROM (VALUES {avis_rows}) "
        "AS v(avis_id, code_commune, nom_ville, date_avis, note_moyenne, "
        "note_environnement, sentiment_avis)"
    )
    con.execute(
        f"CREATE TABLE avis_segments AS SELECT * FROM (VALUES {seg_rows}) "
        "AS v(avis_id, code_commune, date_avis, segment_id, polarity_field, "
        "text, themes, model_score, sentiment, concordant)"
    )
    con.execute(
        f"CREATE TABLE avis_tokens AS SELECT * FROM (VALUES {tok_rows}) "
        "AS v(code_commune, avis_id, segment_id, token, kind, themes, sentiment)"
    )


def _fetch_one(con, col: str, code: str = "'01001'"):
    return con.execute(
        f"SELECT {col} FROM avis_commune WHERE code_commune = {code}"
    ).fetchone()[0]


def test_sentiment_global_is_one_avis_one_vote(con) -> None:
    # Avis A : 3 segments à +0.9 (bavard). Avis B : 1 segment à -0.9.
    # Moyenne par avis : +0.9 et -0.9 → global = 0, malgré 3 vs 1 segments.
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL), "
        "('b','01001','X',DATE '2024-02-01',3.0,NULL,NULL)",
        "('a','01001',DATE '2024-01-01',0,'positif','seg a1',[],0.9,0.9,true), "
        "('a','01001',DATE '2024-01-01',1,'positif','seg a2',[],0.9,0.9,true), "
        "('a','01001',DATE '2024-01-01',2,'positif','seg a3',[],0.9,0.9,true), "
        "('b','01001',DATE '2024-02-01',0,'negatif','seg b1',[],-0.9,-0.9,true)",
        "('01001','a',0,'mot','nom',[],0.9)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    assert _fetch_one(con, "sentiment_global") == pytest.approx(0.0)
    assert _fetch_one(con, "n_avis") == 2


def test_low_data_flag_below_threshold(con) -> None:
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL)",
        "('a','01001',DATE '2024-01-01',0,'positif','un segment ici',[],0.5,0.5,true)",
        "('01001','a',0,'mot','nom',[],0.5)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    assert _fetch_one(con, "low_data") is True  # 1 < MIN_AVIS_THEMES


def test_themes_require_min_segments(con) -> None:
    # 2 segments 'transports' (< MIN_SEGMENTS_THEME=3) → thème écarté.
    seg = ", ".join(
        f"('a{i}','01001',DATE '2024-01-0{i+1}',0,'positif','seg','[\"transports\"]'::VARCHAR[],0.5,0.5,true)"
        for i in range(2)
    )
    avis = ", ".join(
        f"('a{i}','01001','X',DATE '2024-01-0{i+1}',7.0,NULL,NULL)" for i in range(2)
    )
    _seed(con, avis, seg, "('01001','a0',0,'mot','nom',[],0.5)")
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    assert _fetch_one(con, "themes") == []
    assert MIN_SEGMENTS_THEME == 3


def test_wordcloud_weight_dedups_by_avis(con) -> None:
    # 'parc' apparaît 3x dans l'avis a (1 avis) et 1x dans b → weight = 2.
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL), "
        "('b','01001','X',DATE '2024-02-01',7.0,NULL,NULL)",
        "('a','01001',DATE '2024-01-01',0,'positif','seg a',[],0.5,0.5,true), "
        "('b','01001',DATE '2024-02-01',0,'positif','seg b',[],0.5,0.5,true)",
        "('01001','a',0,'parc','nom',[],0.5), "
        "('01001','a',0,'parc','nom',[],0.5), "
        "('01001','a',0,'parc','nom',[],0.5), "
        "('01001','b',0,'parc','nom',[],0.5)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    wc = _fetch_one(con, "wordcloud")
    parc = next(w for w in wc if w["word"] == "parc")
    assert parc["weight"] == 2
    assert parc["sentiment"] == "positive"


def test_verbatim_labels(con) -> None:
    long_pos = "x" * 100  # dans [80, 300]
    long_neg = "y" * 100
    long_nuance = "z" * 100
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL), "
        "('b','01001','X',DATE '2024-02-01',3.0,NULL,NULL), "
        "('c','01001','X',DATE '2024-03-01',5.0,NULL,NULL)",
        f"('a','01001',DATE '2024-01-01',0,'positif','{long_pos}',[],0.8,0.8,true), "
        f"('b','01001',DATE '2024-02-01',0,'negatif','{long_neg}',[],-0.8,-0.8,true), "
        # discordant → Nuancé même si |sentiment| élevé
        f"('c','01001',DATE '2024-03-01',0,'negatif','{long_nuance}',[],0.7,-0.5,false)",
        "('01001','a',0,'mot','nom',[],0.8)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    verbatims = _fetch_one(con, "verbatims")
    by_label = {v["label"]: v for v in verbatims}
    assert set(by_label) == {"Positif", "Négatif", "Nuancé"}
    assert all(v["source"] == "Ville-idéale" for v in verbatims)
    assert by_label["Positif"]["mois"] == "2024-01"


def test_verbatim_max_one_per_avis(con) -> None:
    # Deux segments longs du même avis : un seul verbatim doit sortir.
    seg_text = "w" * 100
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL)",
        f"('a','01001',DATE '2024-01-01',0,'positif','{seg_text}',[],0.8,0.8,true), "
        f"('a','01001',DATE '2024-01-01',1,'positif','{seg_text}2',[],0.6,0.6,true)",
        "('01001','a',0,'mot','nom',[],0.8)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    verbatims = _fetch_one(con, "verbatims")
    assert len({v["text"] for v in verbatims}) == len(verbatims)
    assert len(verbatims) == 1


def test_empty_collections_default_to_lists(con) -> None:
    # Un avis sans token ni verbatim affichable : listes vides, pas NULL.
    _seed(
        con,
        "('a','01001','X',DATE '2024-01-01',7.0,NULL,NULL)",
        "('a','01001',DATE '2024-01-01',0,'positif','court',[],0.5,0.5,true)",
        "('01001','a',0,'mot','nom',[],0.5)",
    )
    avis_commune(con, "avis", "avis_segments", "avis_tokens")
    assert _fetch_one(con, "verbatims") == []  # 'court' < 80 chars
    assert _fetch_one(con, "themes") == []
    assert MIN_AVIS_THEMES == 10
