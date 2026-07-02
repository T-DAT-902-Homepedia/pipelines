#!/usr/bin/env python3
"""Lance le scraping ville-ideale.fr en local (IP résidentielle).

Tourne jusqu'à la fin ou jusqu'à un ban IP.
Si ban : relance le script, il reprend automatiquement où il s'était arrêté.

Usage :
    uv run python scripts/scrape_local.py
    uv run python scripts/scrape_local.py --delay 5
    uv run python scripts/scrape_local.py --max-errors 3
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from homepedia_ville_ideale.scraper import scrape_all  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

HERE = Path(__file__).resolve().parent.parent

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=3.0,
                        help="Délai moyen entre requêtes en secondes (défaut: 3)")
    parser.add_argument("--max-errors", type=int, default=5,
                        help="Arrêt après N erreurs consécutives (défaut: 5)")
    args = parser.parse_args()

    scrape_all(
        communes_csv=HERE / "data" / "communes.csv",
        output_csv=HERE / "data" / "notes_communes.csv",
        html_dir=HERE / "data" / "html_cache",
        delay=args.delay,
        delay_jitter=1.5,
        max_consecutive_errors=args.max_errors,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
