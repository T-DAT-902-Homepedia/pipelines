"""Échantillon DPE ADEME vers le bronze (JSONL).

Adaptation de `exploration/src/ingest_extra.py::ensure_dpe` (partie
téléchargement) : le référentiel DPE complet pèse plusieurs millions de
lignes ; on en récupère un échantillon via l'API data-fair paginée par
scroll (`next` porte l'URL complète de la page suivante, déjà encodée).
"""

from __future__ import annotations

import json
import logging
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

from duckpipe import fetch

logger = logging.getLogger(__name__)

# Chemin bronze relatif du JSONL produit (consommé par catalogs.py).
DPE_BRONZE_PATH = "dpe/dpe_sample.jsonl"

DPE_DATASET = "dpe03existant"
DPE_API = f"https://data.ademe.fr/data-fair/api/v1/datasets/{DPE_DATASET}/lines"
UA = {"User-Agent": "Mozilla/5.0 (homepedia-pipeline)"}
DEFAULT_MAX_ROWS = 200_000
PAGE_SIZE = 10_000


def build_dpe_sample(
    dest: str, *, max_rows: int = DEFAULT_MAX_ROWS, force: bool = False, timeout: int = 60
) -> str:
    """Pagine l'API DPE et écrit un JSONL (code_insee_ban, etiquette_dpe) en
    `dest` (local ou gs://). Idempotent.
    """
    if not force:
        if fetch.is_gcs_uri(dest) and fetch.gcs_exists(dest):
            logger.info("[cache] %s déjà présent", dest)
            return dest
        if not fetch.is_gcs_uri(dest) and Path(dest).exists():
            logger.info("[cache] %s déjà présent", dest)
            return dest

    params = {"size": PAGE_SIZE, "select": "etiquette_dpe,code_insee_ban", "sort": "numero_dpe"}
    url = f"{DPE_API}?{urllib.parse.urlencode(params)}"
    rows: list[dict] = []
    while len(rows) < max_rows:
        request = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
        batch = data.get("results", [])
        if not batch:
            break
        rows.extend(batch)
        logger.info("[dpe] %d lignes récupérées", len(rows))
        next_url = data.get("next")
        if not next_url:
            break
        url = next_url  # l'URL `next` porte déjà tous les paramètres encodés

    if not rows:
        raise RuntimeError("dpe : aucune ligne récupérée, API indisponible ?")

    content = "\n".join(json.dumps(row) for row in rows)
    if fetch.is_gcs_uri(dest):
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            Path(tmp_path).write_text(content, encoding="utf-8")
            fetch.upload_to_gcs(tmp_path, dest)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    else:
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_text(content, encoding="utf-8")

    logger.info("[ok] dpe : %d lignes (échantillon) -> %s", len(rows), dest)
    return dest
