"""
Scraper ville-ideale.fr — technique PyvesB adaptée pour toutes les communes.

Anti-détection :
- tls_client avec fingerprint Chrome 131 (clone exact du TLS navigateur)
- Callback sijs() après chaque page (simule le JS du navigateur)
- Délai aléatoire entre requêtes
- Arrêt automatique après N erreurs consécutives (IP bannie)
- Cache HTML local pour reprendre sans re-scraper

Usage :
    python scripts/scrape_local.py
    python scripts/scrape_local.py --delay 5 --max-errors 3
"""
from __future__ import annotations

import csv
import re
import time
import random
import unicodedata
import logging
from dataclasses import dataclass, fields, asdict
from pathlib import Path

import tls_client

log = logging.getLogger(__name__)

BASE_URL = "https://www.ville-ideale.fr"
SIJS_RE = re.compile(r"sijs\((\d+)\)")

CATEGORY_MAP = {
    "Environnement":    "environnement",
    "Transports":       "transports",
    "Sécurité":         "securite",
    "Santé":            "sante",
    "Sports et loisirs":"sports_et_loisirs",
    "Culture":          "culture",
    "Enseignement":     "enseignement",
    "Commerces":        "commerces",
    "Qualité de vie":   "qualite_de_vie",
}


@dataclass
class CommuneRating:
    slug: str
    code_commune: str
    nom_ville: str
    note_globale: str
    environnement: str
    transports: str
    securite: str
    sante: str
    sports_et_loisirs: str
    culture: str
    enseignement: str
    commerces: str
    qualite_de_vie: str


# ── Slug ──────────────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    slug = unicodedata.normalize("NFD", name.lower())
    slug = "".join(c for c in slug if unicodedata.category(c) != "Mn")
    for ch in ["'", "’", " ", "/"]:
        slug = slug.replace(ch, "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def build_slug(nom_commune: str, code_insee: str) -> str:
    """Ex: 'Metz', '57463' -> 'metz_57463'"""
    code = code_insee[:2].upper() if code_insee[:2].upper() in ("2A", "2B") \
        else str(int(code_insee))
    return f"{slugify(nom_commune)}_{code}"


# ── Session tls_client ────────────────────────────────────────────────────────

def new_session() -> tls_client.Session:
    """Session avec fingerprint Chrome 131 — indiscernable d'un vrai navigateur."""
    return tls_client.Session(
        client_identifier="chrome_131",
        random_tls_extension_order=True,
    )


def send_sijs(session: tls_client.Session, html: str) -> None:
    """POST le callback sijs() que le navigateur envoie après chaque page.
    Sans ça, le serveur détecte le bot après ~10 requêtes.
    """
    m = SIJS_RE.search(html)
    if not m:
        return
    try:
        session.post(
            f"{BASE_URL}/scripts/cherche.php",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": BASE_URL,
                "Referer": f"{BASE_URL}/",
            },
            data=f"click={m.group(1)}",
            timeout_seconds=10,
        )
    except Exception:
        pass


def fetch_page(session: tls_client.Session, url: str) -> str | None:
    try:
        resp = session.get(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
                "Referer": f"{BASE_URL}/",
            },
            timeout_seconds=30,
        )
        if resp.status_code == 200 and len(resp.text) > 500:
            send_sijs(session, resp.text)
            return resp.text
        return None
    except Exception as e:
        log.debug(f"Erreur fetch: {e}")
        return None


# ── Parsing ───────────────────────────────────────────────────────────────────

def parse_ratings(html: str, slug: str, code_commune: str) -> CommuneRating | None:
    # Note globale
    ng = re.search(r'<p\s+id="ng"[^>]*>([\d,]+)', html)
    overall = ng.group(1).replace(",", ".") if ng else ""

    # Nom de la ville
    h1 = re.search(r"<h1>\s*(.+?)\s*(?:\([\d]+\))?\s*</h1>", html)
    nom = h1.group(1).strip().title() if h1 else slug

    # Notes par catégorie dans <table id="tablonotes">
    ratings = {}
    table = re.search(r'<table\s+id="tablonotes">(.*?)</table>', html, re.DOTALL)
    if table:
        for row in re.finditer(
            r"<th[^>]*>\s*(.*?)\s*</th>\s*<td[^>]*>\s*([\d,]+)\s*</td>",
            table.group(1),
        ):
            key = CATEGORY_MAP.get(row.group(1).strip())
            if key:
                ratings[key] = row.group(2).replace(",", ".")

    if not overall and not ratings:
        return None

    return CommuneRating(
        slug=slug,
        code_commune=code_commune,
        nom_ville=nom,
        note_globale=overall,
        **{k: ratings.get(k, "") for k in list(CATEGORY_MAP.values())},
    )


# ── Cache HTML local ──────────────────────────────────────────────────────────

def cache_path(html_dir: Path, slug: str) -> Path:
    return html_dir / f"{slug}.html"


def load_cached(html_dir: Path, slug: str) -> str | None:
    p = cache_path(html_dir, slug)
    return p.read_text(encoding="utf-8") if p.exists() else None


def save_cached(html_dir: Path, slug: str, html: str) -> None:
    html_dir.mkdir(parents=True, exist_ok=True)
    cache_path(html_dir, slug).write_text(html, encoding="utf-8")


# ── Scraping principal ────────────────────────────────────────────────────────

def scrape_all(
    communes_csv: Path,
    output_csv: Path,
    html_dir: Path,
    delay: float = 3.0,
    delay_jitter: float = 1.5,
    max_consecutive_errors: int = 5,
) -> None:
    # Charge les communes
    with open(communes_csv, encoding="utf-8") as f:
        communes = list(csv.DictReader(f))

    # Charge les slugs déjà dans le CSV de sortie (reprise)
    done_slugs: set[str] = set()
    if output_csv.exists():
        with open(output_csv, encoding="utf-8") as f:
            done_slugs = {row["slug"] for row in csv.DictReader(f)}
        log.info(f"Reprise : {len(done_slugs)} communes déjà scrapées")

    todo = []
    for row in communes:
        nom = row.get("nom_commune") or row.get("nom_commune_postal", "")
        code = row.get("code_commune_INSEE", "")
        if not nom or not code:
            continue
        slug = build_slug(nom, code)
        if slug not in done_slugs:
            todo.append((slug, nom, code))

    log.info(f"Communes à scraper : {len(todo)} / {len(communes)}")

    # Ouvre le CSV en mode append
    fieldnames = [f.name for f in fields(CommuneRating)]
    write_header = not output_csv.exists() or output_csv.stat().st_size == 0
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    session = new_session()
    consecutive_errors = 0
    scraped = 0

    with open(output_csv, "a", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for i, (slug, nom, code) in enumerate(todo):
            # Vérifie le cache HTML local d'abord
            html = load_cached(html_dir, slug)
            if html:
                log.debug(f"[{i+1}/{len(todo)}] {nom} — cache")
            else:
                url = f"{BASE_URL}/{slug}"
                time.sleep(max(0.1, random.gauss(delay, delay_jitter)))
                html = fetch_page(session, url)

                if not html:
                    consecutive_errors += 1
                    log.warning(
                        f"[{i+1}/{len(todo)}] {nom} — vide "
                        f"({consecutive_errors}/{max_consecutive_errors})"
                    )
                    if consecutive_errors >= max_consecutive_errors:
                        log.error(
                            f"\n⛔ {max_consecutive_errors} erreurs consécutives — "
                            f"IP probablement bannie.\n"
                            f"Relance le script plus tard pour continuer "
                            f"(progression sauvegardée)."
                        )
                        break
                    continue

                consecutive_errors = 0
                save_cached(html_dir, slug, html)

            rating = parse_ratings(html, slug, code)
            if rating:
                writer.writerow(asdict(rating))
                fout.flush()
                scraped += 1
                log.info(f"[{i+1}/{len(todo)}] {nom} — {rating.note_globale}/10")
            else:
                log.debug(f"[{i+1}/{len(todo)}] {nom} — pas de notes")

    log.info(f"\nTerminé : {scraped} communes avec notes -> {output_csv}")
