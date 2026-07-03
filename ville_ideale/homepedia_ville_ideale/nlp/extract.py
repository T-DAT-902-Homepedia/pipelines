"""Extraction des tokens du nuage de mots depuis un segment.

Successeur du script ``build_wordcloud.py`` (branche wordcloud-pipeline) qui ne
gardait que les adjectifs : ici on veut aussi les noms/noms propres et les
entités-lieux (« La Défense », « RER A ») demandés par la maquette. Le pipeline
spaCy est donc chargé AVEC parser et NER (le parser fournit ``noun_chunks``,
la NER les entités), contrairement à l'ancien script qui les désactivait.

Chaque token extrait porte son ``kind`` (nom | adj | bigramme | entite) ; le
sentiment lui est attribué en aval depuis le segment qui le contient.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spacy.tokens import Doc, Span

MIN_NOUN_LEN = 3
MIN_ADJ_LEN = 4
# Entités de lieu/organisation gardées (RER A, La Défense, Seine…).
_ENTITY_LABELS = {"LOC", "ORG", "GPE"}

# Adjectifs génériques / gentilés à exclure — portée depuis build_wordcloud.py
# (branche massi/wordcloud-pipeline) et étendue à quelques noms passe-partout.
BLACKLIST = {
    "grand", "petit", "bon", "mauvais", "premier", "deuxième", "troisième",
    "certain", "même", "autre", "quelque", "seul", "tel", "tout", "aucun",
    "nombreux", "différent", "nouveau", "vieux", "dernier", "prochain",
    "long", "court", "haut", "bas", "plein", "vrai", "faux", "possible",
    "difficile", "facile", "important", "général", "principal", "simple",
    "normal", "naturel", "national", "international", "public", "privé",
    "parisien", "lyonnais", "marseillais", "bordelais", "lillois",
    # noms trop génériques (bruit de nuage : ni thème ni ressenti)
    "chose", "endroit", "coin", "fois", "année", "gens", "personne", "point",
    "ville", "rue", "côté", "jour", "temps", "monde", "vie", "lieu", "part",
    "niveau", "moment", "cas", "type", "manière", "façon", "nombre", "ensemble",
}


@dataclass(frozen=True)
class TokenRecord:
    """Un token de nuage de mots extrait d'un segment."""

    token: str  # forme affichée (lemme minuscule, ou surface pour les entités)
    kind: str  # nom | adj | bigramme | entite


def _keep_lemma(lemma: str) -> bool:
    return lemma.isalpha() and lemma not in BLACKLIST


def _entity_surface(ent: Span) -> str:
    # Forme de surface nettoyée (les entités gardent leur casse : « La Défense »).
    return " ".join(ent.text.split())


def extract_tokens(doc: Doc) -> list[TokenRecord]:
    """Extrait les tokens de nuage d'un doc spaCy déjà annoté (POS, NER, parser).

    Ordre stable : noms/adjectifs dans l'ordre du texte, puis noun chunks, puis
    entités — un tri déterministe est appliqué en aval de toute façon.
    """
    records: list[TokenRecord] = []
    seen: set[tuple[str, str]] = set()

    def _add(token: str, kind: str) -> None:
        key = (token.lower(), kind)
        if token and key not in seen:
            seen.add(key)
            records.append(TokenRecord(token=token, kind=kind))

    for token in doc:
        lemma = token.lemma_.lower()
        if token.pos_ in ("NOUN", "PROPN") and len(lemma) >= MIN_NOUN_LEN and _keep_lemma(lemma):
            _add(lemma, "nom")
        elif token.pos_ == "ADJ" and len(lemma) >= MIN_ADJ_LEN and _keep_lemma(lemma):
            _add(lemma, "adj")

    # Noun chunks courts (2 tokens) : capture « cadre de vie », « prix élevés ».
    if doc.has_annotation("DEP"):
        for chunk in doc.noun_chunks:
            content = [t for t in chunk if not t.is_stop and not t.is_punct]
            if len(content) == 2:  # noqa: PLR2004 — bigrammes uniquement
                phrase = " ".join(t.lemma_.lower() for t in content)
                if all(t.lemma_.isalpha() for t in content):
                    _add(phrase, "bigramme")

    for ent in doc.ents:
        if ent.label_ in _ENTITY_LABELS:
            _add(_entity_surface(ent), "entite")

    return records
