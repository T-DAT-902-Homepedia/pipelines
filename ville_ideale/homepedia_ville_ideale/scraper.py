"""
Scraper ville-ideale.fr.

Conçu pour tourner dans GitHub Actions : chaque job a une IP Azure différente,
ce qui évite les blocages IP. Les délais aléatoires imitent un comportement humain.

Usage direct :
    python scripts/scrape_ville_ideale.py --start 0 --end 1750 --output /tmp/batch_0.csv
"""
from __future__ import annotations

import csv
import random
import time
import unicodedata
import logging
from dataclasses import dataclass, fields, asdict
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "https://www.ville-ideale.fr"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


@dataclass
class Review:
    code_commune: str
    nom_commune: str
    date: str
    auteur: str
    note_moyenne: str
    environnement: str
    transports: str
    securite: str
    sante: str
    sports_loisirs: str
    culture: str
    enseignement: str
    commerces: str
    qualite_vie: str
    points_positifs: str
    points_negatifs: str


def slugify(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    slug = ascii_str.lower()
    for char in ["'", "'", " ", "/"]:
        slug = slug.replace(char, "-")
    slug = "".join(c for c in slug if c.isalnum() or c == "-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def build_url(nom_commune: str, code_insee: str) -> str:
    slug = slugify(nom_commune)
    code = str(int(code_insee))
    return f"{BASE_URL}/{slug}_{code}"


def parse_reviews(html: str, code_commune: str, nom_commune: str) -> list[Review]:
    soup = BeautifulSoup(html, "html.parser")
    reviews = []

    for div in soup.select("div.comm"):
        p = div.find("p")
        if not p:
            continue
        spans = p.find_all("span")
        date = spans[0].text.replace("Avis posté le ", "").strip() if spans else ""
        strong_tags = p.find_all("strong")
        auteur = strong_tags[0].text.strip() if strong_tags else ""

        note_moy_tag = div.find("strong", class_="moyenne")
        note_moyenne = note_moy_tag.text.strip() if note_moy_tag else ""

        notes = [""] * 9
        tables = div.find_all("table")
        if tables:
            tds = tables[0].find_all("tr")
            if len(tds) > 1:
                vals = tds[1].find_all("td")
                notes = [td.text.strip() for td in vals[:9]]
                notes += [""] * (9 - len(notes))

        pos = neg = ""
        for p_tag in div.find_all("p"):
            b = p_tag.find("b")
            if b and "positif" in b.text.lower():
                pos = p_tag.get_text(separator=" ").replace(b.text, "").strip()
            elif b and "négatif" in b.text.lower():
                neg = p_tag.get_text(separator=" ").replace(b.text, "").strip()

        reviews.append(Review(
            code_commune=code_commune,
            nom_commune=nom_commune,
            date=date,
            auteur=auteur,
            note_moyenne=note_moyenne,
            environnement=notes[0],
            transports=notes[1],
            securite=notes[2],
            sante=notes[3],
            sports_loisirs=notes[4],
            culture=notes[5],
            enseignement=notes[6],
            commerces=notes[7],
            qualite_vie=notes[8],
            points_positifs=pos,
            points_negatifs=neg,
        ))

    return reviews


def scrape_commune(
    client: httpx.Client,
    nom_commune: str,
    code_insee: str,
    max_reviews: int = 10,
) -> list[Review]:
    url = build_url(nom_commune, code_insee)
    reviews: list[Review] = []

    for page in range(1, 3):
        page_url = url if page == 1 else f"{url}?page={page}#commentaires"
        try:
            resp = client.get(page_url, timeout=15)
        except Exception as e:
            log.warning(f"Erreur réseau {nom_commune}: {e}")
            break

        if resp.status_code == 404:
            break
        if resp.status_code != 200:
            log.warning(f"HTTP {resp.status_code}: {nom_commune}")
            break
        if len(resp.content) < 500:
            log.warning(f"Réponse trop courte ({len(resp.content)}b) — IP bloquée ? {nom_commune}")
            break

        page_reviews = parse_reviews(resp.text, code_insee, nom_commune)
        reviews.extend(page_reviews)

        if len(reviews) >= max_reviews or not page_reviews:
            break

        time.sleep(random.gauss(2.0, 0.5))

    return reviews[:max_reviews]


def scrape_batch(
    communes_csv: Path,
    start: int,
    end: int,
    output: Path,
    max_reviews: int = 10,
    delay_min: float = 2.0,
    delay_max: float = 5.0,
) -> None:
    with open(communes_csv, encoding="utf-8") as f:
        communes = list(csv.DictReader(f))

    batch = communes[start:end]
    log.info(f"Batch {start}-{end} : {len(batch)} communes")

    all_reviews: list[Review] = []
    output.parent.mkdir(parents=True, exist_ok=True)

    with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
        for i, row in enumerate(batch):
            nom = row.get("nom_commune") or row.get("nom_commune_postal", "")
            code = row.get("code_commune_INSEE", "")
            if not nom or not code:
                continue

            reviews = scrape_commune(client, nom, code, max_reviews)
            if reviews:
                all_reviews.extend(reviews)
                log.info(f"[{start + i}/{end}] {nom}: {len(reviews)} avis")
            else:
                log.debug(f"[{start + i}/{end}] {nom}: aucun avis")

            time.sleep(random.uniform(delay_min, delay_max))

    fieldnames = [f.name for f in fields(Review)]
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(r) for r in all_reviews)

    log.info(f"Terminé : {len(all_reviews)} avis -> {output}")
