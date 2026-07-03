#!/usr/bin/env python3
"""Extrait les adjectifs des avis via spaCy (fr_core_news_md).

Génère public/data/wordcloud.json pour la webapp.

Usage (depuis ville_ideale/) :
    uv run python scripts/build_wordcloud.py
"""
from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path

import spacy

HERE = Path(__file__).resolve().parent.parent
CSV_PATH = HERE / "data" / "avis_top80.csv"
OUT_PATH = HERE.parent.parent / "webapp" / "public" / "data" / "wordcloud.json"

# Mots à exclure même s'ils sont adjectifs (trop génériques / noms de villes)
BLACKLIST = {
    "grand", "petit", "bon", "mauvais", "premier", "deuxième", "troisième",
    "certain", "même", "autre", "quelque", "seul", "tel", "tout", "aucun",
    "nombreux", "différent", "nouveau", "vieux", "vieux", "dernier", "prochain",
    "long", "court", "haut", "bas", "plein", "vrai", "faux", "possible",
    "difficile", "facile", "important", "général", "principal", "simple",
    "normal", "naturel", "national", "international", "public", "privé",
    "parisien", "lyonnais", "marseillais", "bordelais", "lillois",
}


def extract_adjectives(nlp: spacy.language.Language, text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    doc = nlp(text)
    return [
        token.lemma_.lower()
        for token in doc
        if token.pos_ == "ADJ"
        and len(token.lemma_) >= 4
        and token.lemma_.lower() not in BLACKLIST
        and token.is_alpha
    ]


def main() -> None:
    print("Chargement du modèle spaCy fr_core_news_md…")
    nlp = spacy.load("fr_core_news_md", disable=["ner", "parser"])

    rows = list(csv.DictReader(open(CSV_PATH, encoding="utf-8")))
    print(f"{len(rows)} avis chargés")

    by_slug: dict[str, dict] = {}
    for row in rows:
        slug = row["slug"]
        if slug not in by_slug:
            by_slug[slug] = {"nom_ville": row["nom_ville"], "adj": []}

        for field in ("points_positifs", "points_negatifs"):
            by_slug[slug]["adj"].extend(extract_adjectives(nlp, row.get(field, "")))

    result: dict[str, list[dict]] = {}
    for slug, data in by_slug.items():
        counts = Counter(data["adj"]).most_common(40)
        if not counts:
            continue
        max_c = counts[0][1]
        min_c = counts[-1][1]
        result[slug] = {
            "nom_ville": data["nom_ville"],
            "words": [
                {
                    "word": w,
                    "count": c,
                    "size": 12 if max_c == min_c else round(12 + (c - min_c) / (max_c - min_c) * 36),
                }
                for w, c in counts
            ],
        }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ {len(result)} villes → {OUT_PATH}")

    # Aperçu
    for slug, d in list(result.items())[:3]:
        top = ", ".join(w["word"] for w in d["words"][:8])
        print(f"  {d['nom_ville']}: {top}")


if __name__ == "__main__":
    main()
