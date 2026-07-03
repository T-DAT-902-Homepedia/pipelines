import pytest

from homepedia_ville_ideale.nlp.sentiment import (
    NullBackend,
    PRIOR_ONLY_DAMPING,
    blend,
    stars_to_score,
)


def test_stars_to_score_bounds():
    assert stars_to_score([0, 0, 1, 0, 0]) == 0.0  # 3★ neutre
    assert stars_to_score([0, 0, 0, 0, 1]) == 1.0  # 5★
    assert stars_to_score([1, 0, 0, 0, 0]) == -1.0  # 1★


def test_stars_to_score_uniform_is_neutral():
    assert stars_to_score([0.2] * 5) == pytest.approx(0.0)


def test_stars_to_score_requires_five_classes():
    with pytest.raises(ValueError):
        stars_to_score([0.5, 0.5])


def test_blend_without_model_falls_back_to_damped_prior():
    assert blend(1, None) == pytest.approx(PRIOR_ONLY_DAMPING)
    assert blend(-1, None) == pytest.approx(-PRIOR_ONLY_DAMPING)


def test_blend_combines_prior_and_model():
    # 0.35*1 + 0.65*0.5 = 0.675
    assert blend(1, 0.5) == pytest.approx(0.675)


def test_blend_is_clipped_to_unit_range():
    assert blend(1, 1.0) <= 1.0
    assert blend(-1, -1.0) >= -1.0


def test_blend_detects_contradiction_positive_field_negative_model():
    # champ positif mais modèle très négatif → score négatif (segment nuancé)
    assert blend(1, -0.9) < 0


def test_null_backend_returns_none_for_all():
    assert NullBackend().score_batch(["a", "b", "c"]) == [None, None, None]
    assert NullBackend().score_batch([]) == []
