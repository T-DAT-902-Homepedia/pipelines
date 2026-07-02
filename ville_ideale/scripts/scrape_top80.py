#!/usr/bin/env python3
"""Scrape les avis textuels des 80 plus grandes villes françaises.

- tls_client chrome_131 + callback sijs() pour éviter la détection
- 10 avis max par ville (pages 1 et 2 si nécessaire)
- Résultat : data/avis_top80.csv

Usage (depuis ville_ideale/, hotspot 4G) :
    uv run python scripts/scrape_top80.py
"""
from __future__ import annotations

import csv
import re
import sys
import time
import random
import unicodedata
import logging
from pathlib import Path

import tls_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_URL = "https://www.ville-ideale.fr"
SIJS_RE = re.compile(r"sijs\((\d+)\)")
HERE = Path(__file__).resolve().parent.parent

# Top 80 villes françaises par population (slug ville-ideale, code INSEE)
TOP_80 = [
    ("paris-1er-arrondissement",  "75101"),
    ("marseille-1er-arrondissement", "13201"),
    ("lyon-1er-arrondissement",   "69381"),
    ("toulouse",                  "31555"),
    ("nice",                      "6088"),
    ("nantes",                    "44109"),
    ("montpellier",               "34172"),
    ("strasbourg",                "67482"),
    ("bordeaux",                  "33063"),
    ("lille",                     "59350"),
    ("rennes",                    "35238"),
    ("reims",                     "51454"),
    ("le-havre",                  "76351"),
    ("saint-etienne",             "42218"),
    ("toulon",                    "83137"),
    ("grenoble",                  "38185"),
    ("dijon",                     "21231"),
    ("angers",                    "49007"),
    ("nimes",                     "30189"),
    ("villeurbanne",              "69266"),
    ("le-mans",                   "72181"),
    ("aix-en-provence",           "13001"),
    ("clermont-ferrand",          "63113"),
    ("brest",                     "29019"),
    ("tours",                     "37261"),
    ("limoges",                   "87085"),
    ("amiens",                    "80021"),
    ("perpignan",                 "66136"),
    ("metz",                      "57463"),
    ("besancon",                  "25056"),
    ("boulogne-billancourt",      "92012"),
    ("orleans",                   "45234"),
    ("saint-denis",               "93066"),
    ("argenteuil",                "95018"),
    ("rouen",                     "76540"),
    ("montreuil",                 "93048"),
    ("mulhouse",                  "68224"),
    ("caen",                      "14118"),
    ("nancy",                     "54395"),
    ("saint-paul",                "97411"),
    ("tourcoing",                 "59599"),
    ("roubaix",                   "59512"),
    ("nanterre",                  "92050"),
    ("vitry-sur-seine",           "94081"),
    ("creteil",                   "94028"),
    ("avignon",                   "84007"),
    ("poitiers",                  "86194"),
    ("aubervilliers",             "93001"),
    ("dunkerque",                 "59183"),
    ("aulnay-sous-bois",          "93005"),
    ("asnieres-sur-seine",        "92004"),
    ("colombes",                  "92025"),
    ("versailles",                "78646"),
    ("saint-pierre",              "97414"),
    ("courbevoie",                "92026"),
    ("fort-de-france",            "97209"),
    ("le-tampon",                 "97422"),
    ("rueil-malmaison",           "92063"),
    ("pau",                       "64445"),
    ("champigny-sur-marne",       "94017"),
    ("la-rochelle",               "17300"),
    ("merignac",                  "33281"),
    ("antibes",                   "6004"),
    ("saint-maur-des-fosses",     "94068"),
    ("beziers",                   "34032"),
    ("cannes",                    "6029"),
    ("brive-la-gaillarde",        "19031"),
    ("calais",                    "62193"),
    ("drancy",                    "93029"),
    ("colmar",                    "68066"),
    ("ajaccio",                   "2A004"),
    ("bourges",                   "18033"),
    ("issy-les-moulineaux",       "92040"),
    ("levallois-perret",          "92044"),
    ("la-seyne-sur-mer",          "83126"),
    ("quimper",                   "29232"),
    ("noisy-le-grand",            "93051"),
    ("villeneuve-d-ascq",         "59009"),
    ("troyes",                    "10387"),
]


# ── Session tls_client ────────────────────────────────────────────────────────

def new_session() -> tls_client.Session:
    return tls_client.Session(client_identifier="chrome_131", random_tls_extension_order=True)


def send_sijs(session: tls_client.Session, html: str) -> None:
    m = SIJS_RE.search(html)
    if not m:
        return
    try:
        session.post(
            f"{BASE_URL}/scripts/cherche.php",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Origin": BASE_URL},
            data=f"click={m.group(1)}",
            timeout_seconds=10,
        )
    except Exception:
        pass


def fetch(session: tls_client.Session, url: str) -> str | None:
    try:
        resp = session.get(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Referer": f"{BASE_URL}/",
            },
            timeout_seconds=30,
        )
        if resp.status_code == 200 and len(resp.text) > 500:
            send_sijs(session, resp.text)
            return resp.text
        return None
    except Exception:
        return None


# ── Parsing des avis ──────────────────────────────────────────────────────────

CATEGORY_MAP = {
    "Environnement": "environnement", "Transports": "transports",
    "Sécurité": "securite", "Santé": "sante",
    "Sports et loisirs": "sports_et_loisirs", "Culture": "culture",
    "Enseignement": "enseignement", "Commerces": "commerces",
    "Qualité de vie": "qualite_de_vie",
}

COLUMNS = [
    "slug", "code_commune", "nom_ville", "date", "auteur", "note_moyenne",
    "environnement", "transports", "securite", "sante", "sports_et_loisirs",
    "culture", "enseignement", "commerces", "qualite_de_vie",
    "points_positifs", "points_negatifs",
]


def parse_ville_name(html: str) -> str:
    m = re.search(r"<h1>\s*(.+?)\s*(?:\([\d]+\))?\s*</h1>", html)
    return m.group(1).strip().title() if m else ""


def parse_reviews(html: str, slug: str, code: str, nom_ville: str) -> list[dict]:
    reviews = []
    for div in re.finditer(r'<div class="comm"[^>]*>(.*?)</div>\s*<div', html, re.DOTALL):
        block = div.group(1)

        date_m = re.search(r"Avis posté le (.+?) à", block)
        date = date_m.group(1).strip() if date_m else ""

        auteur_m = re.search(r"Par <strong>(.+?)</strong>", block)
        auteur = auteur_m.group(1).strip() if auteur_m else ""

        moy_m = re.search(r'class="moyenne"[^>]*>([\d,.]+)', block)
        note_moy = moy_m.group(1).replace(",", ".") if moy_m else ""

        # Notes par catégorie
        notes = {}
        table_m = re.search(r"<table>(.*?)</table>", block, re.DOTALL)
        if table_m:
            rows = re.findall(r"<th>(.*?)</th>.*?<td[^>]*>(\d+)</td>", table_m.group(1), re.DOTALL)
            for cat, val in rows:
                key = CATEGORY_MAP.get(cat.replace("<br />", " ").replace("\n", " ").strip())
                if key:
                    notes[key] = val

        # Textes positifs / négatifs
        pos = neg = ""
        for p_m in re.finditer(r"<b>([^<]+)</b>(.*?)(?=<b>|</p>)", block, re.DOTALL):
            label = p_m.group(1).strip().lower()
            text = re.sub(r"<[^>]+>", " ", p_m.group(2)).strip()
            if "positif" in label:
                pos = text
            elif "négatif" in label:
                neg = text

        if not note_moy:
            continue

        reviews.append({
            "slug": slug,
            "code_commune": code,
            "nom_ville": nom_ville,
            "date": date,
            "auteur": auteur,
            "note_moyenne": note_moy,
            **{k: notes.get(k, "") for k in list(CATEGORY_MAP.values())},
            "points_positifs": pos,
            "points_negatifs": neg,
        })
    return reviews


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    output = HERE / "data" / "avis_top80.csv"
    output.parent.mkdir(parents=True, exist_ok=True)

    # Slugs déjà traités (reprise)
    done: set[str] = set()
    if output.exists():
        with open(output, encoding="utf-8") as f:
            done = {r["slug"] for r in csv.DictReader(f)}
        log.info(f"Reprise : {len(done)} villes déjà scrapées")

    todo = [(s, c) for s, c in TOP_80 if s not in done]
    log.info(f"Villes à scraper : {len(todo)}")

    session = new_session()
    write_header = not output.exists() or output.stat().st_size == 0

    with open(output, "a", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=COLUMNS)
        if write_header:
            writer.writeheader()

        consecutive_errors = 0
        for i, (slug, code) in enumerate(todo):
            all_reviews: list[dict] = []

            for page in range(1, 3):  # max 2 pages = ~14-16 avis
                url = f"{BASE_URL}/{slug}_{code}" if page == 1 \
                    else f"{BASE_URL}/{slug}_{code}?page={page}#commentaires"

                time.sleep(max(0.5, random.gauss(3.0, 1.0)))
                html = fetch(session, url)

                if not html:
                    consecutive_errors += 1
                    log.warning(f"[{i+1}/{len(todo)}] {slug} p{page} — vide ({consecutive_errors}/5)")
                    if consecutive_errors >= 5:
                        log.error("⛔ IP bannie — relance depuis hotspot 4G")
                        return 1
                    break

                consecutive_errors = 0
                nom_ville = parse_ville_name(html)
                page_reviews = parse_reviews(html, slug, code, nom_ville)
                all_reviews.extend(page_reviews)

                if len(all_reviews) >= 10 or not page_reviews:
                    break

            for r in all_reviews[:10]:
                writer.writerow(r)
            fout.flush()

            log.info(f"[{i+1}/{len(todo)}] {slug} — {len(all_reviews[:10])} avis")

    log.info(f"Terminé -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
