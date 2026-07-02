#!/usr/bin/env python3
"""Script de scraping ville-ideale.fr — un batch de communes.

Usage (depuis ville_ideale/) :
    uv run python scripts/scrape_ville_ideale.py \
        --communes ../data/communes.csv \
        --start 0 --end 1750 \
        --output /tmp/batch_0.csv

Variables d'environnement :
    MAX_REVIEWS   nombre max d'avis par ville (défaut 10)
    DELAY_MIN     délai minimum entre requêtes en secondes (défaut 2.0)
    DELAY_MAX     délai maximum entre requêtes en secondes (défaut 5.0)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_ville_ideale.scraper import scrape_batch  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--communes", required=True, type=Path)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    with open(args.communes, encoding="utf-8") as f:
        total = sum(1 for _ in f) - 1

    end = args.end if args.end is not None else total

    scrape_batch(
        communes_csv=args.communes,
        start=args.start,
        end=end,
        output=args.output,
        max_reviews=int(os.getenv("MAX_REVIEWS", "10")),
        delay_min=float(os.getenv("DELAY_MIN", "2.0")),
        delay_max=float(os.getenv("DELAY_MAX", "5.0")),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
