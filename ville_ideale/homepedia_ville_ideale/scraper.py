"""Scraping des avis ville-ideale.fr via curl_cffi (empreinte navigateur).

curl_cffi (impersonate chrome) reproduit l'empreinte TLS **et** HTTP/2 d'un vrai
Chrome — c'est ce que JA4H regarde en 2026, là où requests/BeautifulSoup se
font repérer d'entrée. Combiné à une rotation d'IP (cf. driver
``scripts/scrape_ville_ideale.py`` et le workflow matrix GitHub), c'est la
parade retenue au rate limiting (cf. plan avis-sentiment).

Le parsing est le même que l'ancien ``scrape_top80.py`` mais corrige deux bugs
qui bornaient le corpus :
- le dernier avis de chaque page était perdu (la regex de bloc exigeait un
  ``<div`` suivant) ;
- seule la 1ʳᵉ note de catégorie était captée (``.*?`` + DOTALL traversait les
  lignes du tableau).

Le module de parsing (``parse_reviews``, ``parse_ville_name``) est pur et
testable hors-ligne ; la partie réseau (``Scraper``) importe curl_cffi
paresseusement.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

BASE_URL = "https://www.ville-ideale.fr"
SIJS_RE = re.compile(r"sijs\((\d+)\)")

# Un avis = un bloc <div class="comm">…</div>. On borne chaque bloc au prochain
# <div class="comm"> OU à la fin de la zone de commentaires : ainsi le DERNIER
# avis de la page n'est plus perdu (bug de scrape_top80).
_COMM_BLOCK = re.compile(
    r'<div class="comm"[^>]*>(.*?)(?=<div class="comm"|<div id="pagination"|</section>|</main>|\Z)',
    re.DOTALL,
)
_DATE = re.compile(r"Avis posté le (.+?) à")
_AUTEUR = re.compile(r"Par <strong>(.+?)</strong>")
_MOYENNE = re.compile(r'class="moyenne"[^>]*>([\d,.]+)')
_TABLE = re.compile(r"<table>(.*?)</table>", re.DOTALL)
# Chaque ligne du tableau de notes indépendamment (pas de DOTALL qui déborderait
# sur la ligne suivante et n'en capturerait qu'une seule).
_NOTE_ROW = re.compile(r"<th>(.*?)</th>\s*<td[^>]*>(\d+)</td>")
_LABELLED_TEXT = re.compile(r"<b>([^<]+)</b>(.*?)(?=<b>|</p>)", re.DOTALL)
_H1 = re.compile(r"<h1>\s*(.+?)\s*(?:\([\d]+\))?\s*</h1>")
_TAG = re.compile(r"<[^>]+>")

CATEGORY_MAP = {
    "Environnement": "environnement",
    "Transports": "transports",
    "Sécurité": "securite",
    "Santé": "sante",
    "Sports et loisirs": "sports_et_loisirs",
    "Culture": "culture",
    "Enseignement": "enseignement",
    "Commerces": "commerces",
    "Qualité de vie": "qualite_de_vie",
}

COLUMNS = [
    "slug", "code_commune", "nom_ville", "date", "auteur", "note_moyenne",
    "environnement", "transports", "securite", "sante", "sports_et_loisirs",
    "culture", "enseignement", "commerces", "qualite_de_vie",
    "points_positifs", "points_negatifs",
]


def parse_ville_name(html: str) -> str:
    m = _H1.search(html)
    return m.group(1).strip().title() if m else ""


def _parse_notes(block: str) -> dict[str, str]:
    notes: dict[str, str] = {}
    table_m = _TABLE.search(block)
    if not table_m:
        return notes
    for cat, val in _NOTE_ROW.findall(table_m.group(1)):
        key = CATEGORY_MAP.get(cat.replace("<br />", " ").replace("\n", " ").strip())
        if key:
            notes[key] = val
    return notes


def _parse_texts(block: str) -> tuple[str, str]:
    pos = neg = ""
    for label_m in _LABELLED_TEXT.finditer(block):
        label = label_m.group(1).strip().lower()
        text = _TAG.sub(" ", label_m.group(2)).strip()
        if "positif" in label:
            pos = text
        elif "négatif" in label or "negatif" in label:
            neg = text
    return pos, neg


def parse_reviews(html: str, slug: str, code: str, nom_ville: str) -> list[dict]:
    """Extrait tous les avis d'une page (dernier avis inclus)."""
    reviews = []
    for block_m in _COMM_BLOCK.finditer(html):
        block = block_m.group(1)

        moy_m = _MOYENNE.search(block)
        if not moy_m:
            continue  # bloc sans note = pas un avis exploitable

        date_m = _DATE.search(block)
        auteur_m = _AUTEUR.search(block)
        notes = _parse_notes(block)
        pos, neg = _parse_texts(block)

        reviews.append({
            "slug": slug,
            "code_commune": code,
            "nom_ville": nom_ville,
            "date": date_m.group(1).strip() if date_m else "",
            "auteur": auteur_m.group(1).strip() if auteur_m else "",
            "note_moyenne": moy_m.group(1).replace(",", "."),
            **{k: notes.get(k, "") for k in CATEGORY_MAP.values()},
            "points_positifs": pos,
            "points_negatifs": neg,
        })
    return reviews


def page_url(slug: str, code: str, page: int) -> str:
    if page == 1:
        return f"{BASE_URL}/{slug}_{code}"
    return f"{BASE_URL}/{slug}_{code}?page={page}#commentaires"


@dataclass
class Scraper:
    """Session curl_cffi (empreinte Chrome) avec callback anti-bot ``sijs``.

    ``proxies`` permet de router via un pool (rotation d'IP) ; None = IP directe.
    """

    impersonate: str = "chrome124"
    proxies: dict[str, str] | None = None
    timeout: int = 30
    _session: object = None

    def __post_init__(self) -> None:
        from curl_cffi import requests as cffi_requests  # noqa: PLC0415

        self._session = cffi_requests.Session(
            impersonate=self.impersonate, proxies=self.proxies
        )

    def _send_sijs(self, html: str) -> None:
        m = SIJS_RE.search(html)
        if not m:
            return
        try:
            self._session.post(
                f"{BASE_URL}/scripts/cherche.php",
                headers={"Content-Type": "application/x-www-form-urlencoded", "Origin": BASE_URL},
                data=f"click={m.group(1)}",
                timeout=10,
            )
        except Exception:  # noqa: BLE001 — best effort anti-bot
            pass

    def fetch(self, url: str) -> str | None:
        try:
            resp = self._session.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "fr-FR,fr;q=0.9",
                    "Referer": f"{BASE_URL}/",
                },
                timeout=self.timeout,
            )
        except Exception:  # noqa: BLE001 — réseau : traité comme page vide
            return None
        if resp.status_code == 200 and len(resp.text) > 500:  # noqa: PLR2004
            self._send_sijs(resp.text)
            return resp.text
        return None
