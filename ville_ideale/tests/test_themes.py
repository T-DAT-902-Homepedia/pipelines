"""Tests du lexique de thèmes (données pures) + matcher spaCy (importorskip)."""

import pytest

from homepedia_ville_ideale.nlp.themes import THEME_IDS, THEME_LEXICON


def test_six_themes_defined():
    assert set(THEME_IDS) == {
        "securite", "calme", "transports", "commerces", "education", "environnement",
    }


def test_every_theme_has_a_lexicon():
    for theme_id in THEME_IDS:
        assert THEME_LEXICON[theme_id], f"lexique vide pour {theme_id}"


def test_calme_is_text_only_not_a_rating_category():
    # "calme" n'est pas une catégorie de note ville-ideale : il doit exister
    # dans le lexique (donc détectable depuis le texte seul).
    assert "calme" in THEME_LEXICON["calme"]


@pytest.fixture(scope="module")
def nlp():
    spacy = pytest.importorskip("spacy")
    try:
        return spacy.load("fr_core_news_md")
    except OSError:
        pytest.skip("modèle fr_core_news_md non installé")


def test_matcher_tags_expected_themes(nlp):
    from homepedia_ville_ideale.nlp.themes import build_matcher, tag_themes

    matcher = build_matcher(nlp)
    doc = nlp("Le métro est pratique mais le stationnement reste compliqué.")
    assert "transports" in tag_themes(doc, matcher)


def test_matcher_multiword_and_accents(nlp):
    from homepedia_ville_ideale.nlp.themes import build_matcher, tag_themes

    matcher = build_matcher(nlp)
    doc = nlp("Beaucoup d'espaces verts et une belle propreté générale.")
    assert "environnement" in tag_themes(doc, matcher)


def test_tag_themes_returns_canonical_order(nlp):
    from homepedia_ville_ideale.nlp.themes import build_matcher, tag_themes

    matcher = build_matcher(nlp)
    doc = nlp("Commerces variés, quartier calme, transports au top.")
    themes = tag_themes(doc, matcher)
    # ordre canonique THEME_IDS (calme avant transports avant commerces)
    assert themes == [t for t in THEME_IDS if t in set(themes)]
