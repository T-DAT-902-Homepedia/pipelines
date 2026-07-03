"""Découpe des textes d'avis en segments de phrase.

Les avis ville-ideale placent le ressenti positif dans ``points_positifs`` et
le négatif dans ``points_negatifs`` : cette séparation par champ est le signal
structurel de polarité (prior) exploité en aval. On segmente ensuite chaque
champ en unités de phrase pour scorer et taguer finement, car un même champ
peut mélanger plusieurs aspects (« calme mais mal desservi »).

Découpage : puces ``→`` fréquentes dans le corpus, sauts de ligne, puis
ponctuation de fin de phrase. Les segments trop courts (< MIN_SEGMENT_CHARS)
sont écartés — bruit (« Rien », « RAS ») sans valeur pour le sentiment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Un segment plus court est écarté : trop peu de contenu pour un score fiable.
MIN_SEGMENT_CHARS = 15

# Séparateurs « durs » : puces et retours à la ligne découpent toujours.
_HARD_SPLIT = re.compile(r"[→▶•\n\r]+")  # → ▶ • + newlines
# Ponctuation de fin de phrase (on garde une segmentation simple, sans spaCy :
# le sentence splitter du parser spaCy est plus lourd et n'apporte rien ici).
_SENT_SPLIT = re.compile(r"(?<=[.!?…])\s+")
_WS = re.compile(r"\s+")

POSITIF = "positif"
NEGATIF = "negatif"


@dataclass(frozen=True)
class RawSegment:
    """Un segment de phrase issu d'un champ d'avis, avec sa polarité de champ."""

    polarity_field: str  # POSITIF | NEGATIF
    order: int  # position du segment dans l'avis (0-based), pour un tri déterministe
    text: str


def _normalize(text: str) -> str:
    return _WS.sub(" ", text).strip()


def segment_text(text: str) -> list[str]:
    """Découpe un texte libre en segments de phrase nettoyés.

    Renvoie une liste (éventuellement vide) de segments d'au moins
    MIN_SEGMENT_CHARS caractères, dans l'ordre du texte.
    """
    if not text:
        return []
    segments: list[str] = []
    for chunk in _HARD_SPLIT.split(text):
        for sentence in _SENT_SPLIT.split(chunk):
            cleaned = _normalize(sentence)
            if len(cleaned) >= MIN_SEGMENT_CHARS:
                segments.append(cleaned)
    return segments


def segment_review(points_positifs: str, points_negatifs: str) -> list[RawSegment]:
    """Segmente les deux champs d'un avis en conservant la polarité de champ.

    L'``order`` est global à l'avis (positifs puis négatifs) pour donner un
    critère de tri stable aux étapes aval (déterminisme, cf. ADR-0008).
    """
    out: list[RawSegment] = []
    order = 0
    for field_value, polarity in ((points_positifs, POSITIF), (points_negatifs, NEGATIF)):
        for text in segment_text(field_value or ""):
            out.append(RawSegment(polarity_field=polarity, order=order, text=text))
            order += 1
    return out
