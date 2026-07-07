"""Détection des thèmes d'un segment par lexique (aspect matching).

Six thèmes attendus par la maquette (écran « Analyse textuelle »). « Calme »
n'existe pas comme catégorie de note ville-ideale : il ne peut venir que du
texte, d'où l'approche lexique plutôt que réutilisation des notes.

Le matching se fait sur les lemmes via un ``PhraseMatcher`` spaCy (attribut
LEMMA) : robuste aux accords/pluriels et gère les expressions multi-mots
(« espace vert », « nuisance sonore »). Les lexiques sont de simples données,
testables sans spaCy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spacy.language import Language
    from spacy.matcher import PhraseMatcher
    from spacy.tokens import Doc

# Identifiants stables des thèmes (clés portées jusqu'au front). L'ordre fixe
# l'ordre d'apparition dans la liste `themes` d'un segment (déterminisme).
THEME_IDS = ("securite", "calme", "transports", "commerces", "education", "environnement")
THEME_ID_SET = frozenset(THEME_IDS)

# Lexiques par thème : formes de surface, lemmatisées à la construction du
# matcher. Volontairement compacts et orientés vocabulaire d'avis d'habitants.
THEME_LEXICON: dict[str, tuple[str, ...]] = {
    "securite": (
        "sécurité", "insécurité", "vol", "cambriolage", "agression", "agresser",
        "drogue", "dealer", "délinquance", "délinquant", "police", "incivilité",
        "violence", "racaille", "trafic",
    ),
    "calme": (
        "calme", "tranquille", "tranquillité", "paisible", "reposant", "serein",
        "bruit", "bruyant", "nuisance sonore", "tapage", "tapage nocturne",
        "vacarme", "silencieux",
    ),
    "transports": (
        "transport", "métro", "tramway", "tram", "bus", "rer", "gare", "train",
        "stationnement", "parking", "se garer", "piste cyclable", "vélo",
        "embouteillage", "circulation", "desservi", "desserte", "autoroute",
    ),
    "commerces": (
        "commerce", "magasin", "boutique", "marché", "supermarché", "restaurant",
        "bar", "boulangerie", "cafe", "café", "commerçant", "commercial",
    ),
    "education": (
        "école", "collège", "lycée", "université", "crèche", "enseignement",
        "scolaire", "éducation", "établissement scolaire",
    ),
    "environnement": (
        "propreté", "propre", "sale", "saleté", "pollution", "pollué", "déchet",
        "poubelle", "espace vert", "parc", "jardin", "nature", "verdure", "arbre",
    ),
}


def build_matcher(nlp: Language) -> PhraseMatcher:
    """Construit un ``PhraseMatcher`` (attribut LEMMA) chargé de tous les thèmes.

    Chaque thème est un label du matcher ; on tokenise chaque entrée de lexique
    avec la même pipeline spaCy pour comparer lemme à lemme.
    """
    from spacy.matcher import PhraseMatcher  # noqa: PLC0415 — dépendance optionnelle

    matcher = PhraseMatcher(nlp.vocab, attr="LEMMA")
    for theme_id in THEME_IDS:
        patterns = list(nlp.pipe(THEME_LEXICON[theme_id]))
        matcher.add(theme_id, patterns)
    return matcher


def tag_themes(doc: Doc, matcher: PhraseMatcher) -> list[str]:
    """Renvoie les thèmes présents dans un doc, dédupliqués, dans l'ordre
    canonique de ``THEME_IDS`` (déterministe, indépendant de l'ordre des
    matchs)."""
    found: set[str] = set()
    for match_id, _start, _end in matcher(doc):
        found.add(doc.vocab.strings[match_id])
    return [theme_id for theme_id in THEME_IDS if theme_id in found]
