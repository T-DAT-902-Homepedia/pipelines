"""Scoring de sentiment d'un segment, dans [-1, +1].

Approche hybride :

- **prior structurel** : un segment issu de ``points_positifs`` vaut +1, de
  ``points_negatifs`` vaut -1. C'est le signal le plus fiable du corpus (les
  habitants trient eux-mêmes leur ressenti).
- **modèle** : ``cmarkea/distilcamembert-base-sentiment`` (5 étoiles) affine au
  niveau segment et permet de détecter les segments « nuancés » (un négatif
  peut contenir une réserve modérée). Sa sortie 5 classes est repliée sur
  [-1, +1] par ``stars_to_score``.

Le modèle est optionnel (Protocol ``SentimentBackend``) : ``NullBackend``
renvoie None partout, et ``blend`` retombe alors sur le seul prior amorti. Les
tests utilisent un backend factice sans jamais charger torch.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

# Modèle et révision épinglés pour la reproductibilité (cf. plan, risques).
MODEL_NAME = "cmarkea/distilcamembert-base-sentiment"
MODEL_REVISION = "main"

# Pondération du blend prior/modèle. Le modèle domine (nuance fine) mais le
# prior garde du poids car c'est le signal auto-déclaré le plus sûr.
W_PRIOR = 0.35
W_MODEL = 0.65
# Sans modèle, on amortit le prior (±0.6) : un signal binaire brut sur-estimerait
# la confiance (tout serait ±1) et fausserait les seuils Nuancé/pos/neg aval.
PRIOR_ONLY_DAMPING = 0.6


@runtime_checkable
class SentimentBackend(Protocol):
    """Score une liste de textes en [-1, +1] ; None si non scorable."""

    def score_batch(self, texts: Sequence[str]) -> list[float | None]: ...


class NullBackend:
    """Backend sans modèle : aucun texte n'est scoré (mode ``--no-model``)."""

    def score_batch(self, texts: Sequence[str]) -> list[float | None]:
        return [None] * len(texts)


def stars_to_score(probs: Sequence[float]) -> float:
    """Replie une distribution softmax 5 étoiles (1★..5★) sur [-1, +1].

    Espérance du nombre d'étoiles ``E = Σ pᵢ·(i+1)`` (i de 0 à 4), recentrée :
    ``(E - 3) / 2``. 3★ (neutre) → 0, 5★ → +1, 1★ → -1.
    """
    if len(probs) != 5:  # noqa: PLR2004 — le modèle a exactement 5 classes
        raise ValueError(f"attendu 5 probabilités (étoiles), reçu {len(probs)}")
    expected_stars = sum(p * (i + 1) for i, p in enumerate(probs))
    return (expected_stars - 3.0) / 2.0


def blend(prior: int, model_score: float | None) -> float:
    """Combine le prior de champ (+1/-1) et le score modèle en [-1, +1].

    Sans score modèle, retombe sur le prior amorti. Le résultat est borné.
    """
    if model_score is None:
        return prior * PRIOR_ONLY_DAMPING
    combined = W_PRIOR * prior + W_MODEL * model_score
    return max(-1.0, min(1.0, combined))


class TransformersBackend:
    """Backend DistilCamemBERT (5 étoiles). Importe torch/transformers à la
    construction uniquement : rien n'est chargé tant qu'on ne l'instancie pas.
    """

    def __init__(
        self,
        model_name: str = MODEL_NAME,
        revision: str = MODEL_REVISION,
        *,
        batch_size: int = 32,
        max_length: int = 512,
    ) -> None:
        import torch  # noqa: PLC0415 — dépendance optionnelle "nlp"
        from transformers import (  # noqa: PLC0415
            AutoModelForSequenceClassification,
            AutoTokenizer,
        )

        self._torch = torch
        self.batch_size = batch_size
        self.max_length = max_length
        self.tokenizer = AutoTokenizer.from_pretrained(model_name, revision=revision)
        self.model = AutoModelForSequenceClassification.from_pretrained(
            model_name, revision=revision
        )
        self.model.eval()

    def score_batch(self, texts: Sequence[str]) -> list[float | None]:
        if not texts:
            return []
        torch = self._torch
        scores: list[float | None] = []
        for start in range(0, len(texts), self.batch_size):
            batch = list(texts[start : start + self.batch_size])
            enc = self.tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=self.max_length,
                return_tensors="pt",
            )
            with torch.no_grad():
                logits = self.model(**enc).logits
                probs = torch.softmax(logits, dim=-1)
            for row in probs.tolist():
                scores.append(stars_to_score(row))
        return scores
