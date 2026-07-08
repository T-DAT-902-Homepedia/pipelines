from homepedia_ville_ideale.nlp.segmenter import (
    MIN_SEGMENT_CHARS,
    NEGATIF,
    POSITIF,
    segment_review,
    segment_text,
)


def test_splits_on_arrow_bullets_and_newlines():
    text = "→ Les transports sont exceptionnels.\n→ Commerces variés et proches."
    segments = segment_text(text)
    assert len(segments) == 2
    assert segments[0].startswith("Les transports")
    assert segments[1].startswith("Commerces")


def test_splits_multiline_paragraph_into_sentences():
    text = "Quartier calme et familial. Les bords de Seine sont superbes le week-end."
    segments = segment_text(text)
    assert len(segments) == 2


def test_drops_short_noise_segments():
    # "RAS" et "Rien" sont sous le seuil et doivent disparaître.
    assert segment_text("RAS") == []
    assert segment_text("Rien") == []
    assert all(len(s) >= MIN_SEGMENT_CHARS for s in segment_text("Rien. Le calme absolu ici."))


def test_normalizes_whitespace():
    (seg,) = segment_text("Le    calme\t\tabsolu   règne ici")
    assert "  " not in seg


def test_segment_review_tags_polarity_and_order():
    segs = segment_review("Calme et verdoyant partout.", "Stationnement vraiment compliqué.")
    assert [s.polarity_field for s in segs] == [POSITIF, NEGATIF]
    assert [s.order for s in segs] == [0, 1]


def test_segment_review_handles_empty_fields():
    assert segment_review("", "") == []
    assert segment_review(None, None) == []
