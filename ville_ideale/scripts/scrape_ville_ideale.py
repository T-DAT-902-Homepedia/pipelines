#!/usr/bin/env python3
"""Scrape les avis textuels de ville-ideale.fr — France entière.

Successeur de scrape_top80.py : curl_cffi (empreinte Chrome, contourne JA4H),
pagination complète (plus 2 pages max), et rotation d'IP optionnelle pour tenir
le débit sur ~35 000 communes sans se faire bloquer (cf. plan avis-sentiment).

Stratégie anti-blocage (par ordre de préférence, coût ~0 d'abord) :
  1. curl_cffi seul (empreinte navigateur) — suffisant à petit débit.
  2. --aws-rotate : rotation d'IP gratuite via AWS API Gateway
     (requests-ip-rotator, extra `rotate`). Pseudo-illimité dans les free tiers.
  3. workflow GitHub matrix (scrape-ville-ideale.yml) : 1 IP Azure par job.
Toujours : délai gaussien 3-5 s + jitter, backoff, détection de ban.

Usage :
    uv run python scripts/scrape_ville_ideale.py \
        --communes data/communes.csv --output data/avis_france.csv \
        [--start 0 --end 2000] [--max-pages 15] [--aws-rotate]

Le CSV de communes doit avoir les colonnes `slug` et `code_commune` (dérivables
de la liste data.gouv des communes, cf. README).
"""
from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
import time
from pathlib import Path

# Le package est importable quand on lance depuis ville_ideale/ (uv run).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_ville_ideale.scraper import (  # noqa: E402
    COLUMNS,
    Scraper,
    page_url,
    parse_reviews,
    parse_ville_name,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MAX_PAGES = 15  # garde-fou : une commune dépasse rarement ; stop dès page vide
BAN_STREAK = 8  # pages vides consécutives -> IP probablement bannie
DELAY_MEAN = 3.5
DELAY_STD = 1.0


def load_communes(path: Path) -> list[tuple[str, str]]:
    """Charge (slug, code) depuis un CSV `slug,code_commune`.

    Tolère les slugs déjà suffixés du code (`amberieu-en-bugey_1004`, format de
    `notes_par_commune.csv`) : le suffixe `_<code>` est retiré, le scraper le
    reconstruit via `page_url`.
    """
    communes: list[tuple[str, str]] = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug, code = row.get("slug"), row.get("code_commune")
            if not slug or not code:
                continue
            code = code.zfill(5)
            suffix = f"_{row['code_commune']}"
            if slug.endswith(suffix):
                slug = slug[: -len(suffix)]
            communes.append((slug, code))
    return communes


def already_done(output: Path) -> set[str]:
    if not output.exists():
        return set()
    with output.open(encoding="utf-8") as f:
        return {row["slug"] for row in csv.DictReader(f)}


def _make_scraper(aws_rotate: bool) -> Scraper:
    if not aws_rotate:
        return Scraper()
    # requests-ip-rotator installe une passerelle AWS et route les requêtes ;
    # curl_cffi ne l'utilise pas directement, donc on retombe sur un proxy si
    # fourni. Ici on documente : le rotate AWS s'emploie via la lib requests.
    log.warning("--aws-rotate : à câbler avec requests-ip-rotator (extra `rotate`)")
    return Scraper()


def scrape_commune(scraper: Scraper, slug: str, code: str, max_pages: int) -> list[dict]:
    """Pagine une commune jusqu'à épuisement (page vide) ou max_pages."""
    all_reviews: list[dict] = []
    nom_ville = ""
    for page in range(1, max_pages + 1):
        time.sleep(max(0.5, random.gauss(DELAY_MEAN, DELAY_STD)))  # noqa: S311
        html = scraper.fetch(page_url(slug, code, page))
        if not html:
            return all_reviews  # page vide / erreur : on s'arrête là
        if not nom_ville:
            nom_ville = parse_ville_name(html)
        page_reviews = parse_reviews(html, slug, code, nom_ville)
        if not page_reviews:
            break  # plus d'avis : commune épuisée
        all_reviews.extend(page_reviews)
    return all_reviews


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--communes", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES)
    parser.add_argument("--aws-rotate", action="store_true")
    args = parser.parse_args()

    communes = load_communes(args.communes)[args.start : args.end]
    done = already_done(args.output)
    todo = [(s, c) for s, c in communes if s not in done]
    log.info("%d communes à scraper (%d déjà faites)", len(todo), len(done))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    scraper = _make_scraper(args.aws_rotate)
    write_header = not args.output.exists() or args.output.stat().st_size == 0

    with args.output.open("a", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=COLUMNS)
        if write_header:
            writer.writeheader()

        empty_streak = 0
        for i, (slug, code) in enumerate(todo):
            reviews = scrape_commune(scraper, slug, code, args.max_pages)
            for review in reviews:
                writer.writerow(review)
            fout.flush()

            empty_streak = empty_streak + 1 if not reviews else 0
            log.info("[%d/%d] %s — %d avis", i + 1, len(todo), slug, len(reviews))
            if empty_streak >= BAN_STREAK:
                log.error("⛔ %d communes vides d'affilée — IP probablement bannie", empty_streak)
                return 1

    log.info("Terminé -> %s", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
