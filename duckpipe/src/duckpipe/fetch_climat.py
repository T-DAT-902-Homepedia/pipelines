"""Construction du CSV des stations climatologiques Météo-France (bronze climat).

Adaptation de `exploration/src/ingest_extra.py::_parse_fiche_data` et
`_build_stations_csv` : Météo-France ne publie pas de dataset tabulaire des
normales 1991-2020 — on télécharge la liste des stations (GeoJSON) puis la
fiche texte `.data` de chaque station (~600 requêtes, ~5 min) dont on extrait
lat/lon, insolation annuelle et température moyenne par parsing regex.
"""

from __future__ import annotations

import csv
import json
import logging
import re
import tempfile
import urllib.request
from pathlib import Path

from duckpipe import fetch

logger = logging.getLogger(__name__)

# Chemin bronze relatif du CSV produit (consommé par catalogs.py).
CLIMAT_BRONZE_PATH = "climat/fiches_climatologiques_stations.csv"

STATIONS_GEOJSON_URL = (
    "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/"
    "REF_STATION/liste_fiches_clim.geojson"
)
FICHE_URL_TEMPLATE = (
    "https://meteofrance.s3.sbg.io.cloud.ovh.net/data/synchro_ftp/"
    "REF_STATION/FICHECLIM_{num}.data"
)
CSV_FIELDS = ["num_poste", "lat", "lon", "ensoleillement_h_an", "temperature_moy_annuelle"]
# Les coordonnées sont toujours sur la 4e ligne des fiches .data Météo-France.
HEADER_LINE_INDEX = 3


def parse_fiche_data(text: str) -> dict:
    """Extrait lat, lon, insolation annuelle et température d'une fiche `.data`."""
    result: dict = {}
    lines = text.split("\n")

    header = lines[HEADER_LINE_INDEX] if len(lines) > HEADER_LINE_INDEX else ""
    m = re.search(r"lat\s*:\s*(\d+)°(\d+)'(\d+)\"([NS])", header)
    if m:
        deg, mn, sec, hemi = m.groups()
        lat = int(deg) + int(mn) / 60 + int(sec) / 3600
        result["lat"] = round(-lat if hemi == "S" else lat, 5)

    m = re.search(r"lon\s*:\s*(\d+)°(\d+)'(\d+)\"([EW])", header)
    if m:
        deg, mn, sec, hemi = m.groups()
        lon = int(deg) + int(mn) / 60 + int(sec) / 3600
        result["lon"] = round(-lon if hemi == "W" else lon, 5)

    def _last_row_of_12_numbers(start: int) -> float | None:
        for j in range(start + 1, min(start + 4, len(lines))):
            nums = []
            for part in lines[j].split(";"):
                try:
                    nums.append(float(part.strip().replace(",", ".")))
                except ValueError:
                    pass
            if len(nums) >= 12:  # noqa: PLR2004 — 12 mois + total annuel
                return nums[-1]
        return None

    for i, line in enumerate(lines):
        if "insolation" in line.lower() and "heures" in line.lower():
            value = _last_row_of_12_numbers(i)
            if value is not None:
                result["ensoleillement_h_an"] = value
            break

    for i, line in enumerate(lines):
        if "Température moyenne (Moyenne" in line:
            value = _last_row_of_12_numbers(i)
            if value is not None:
                result["temperature_moy_annuelle"] = value
            break

    return result


def build_stations_csv(dest: str, *, force: bool = False, timeout: int = 15) -> str:
    """Télécharge et parse toutes les fiches stations -> CSV en `dest`
    (local ou gs://). Idempotent. Les stations dont la fiche est illisible ou
    incomplète sont ignorées (comme dans la référence).
    """
    if not force:
        if fetch.is_gcs_uri(dest) and fetch.gcs_exists(dest):
            logger.info("[cache] %s déjà présent", dest)
            return dest
        if not fetch.is_gcs_uri(dest) and Path(dest).exists():
            logger.info("[cache] %s déjà présent", dest)
            return dest

    with urllib.request.urlopen(STATIONS_GEOJSON_URL, timeout=timeout) as response:
        stations = json.loads(response.read().decode("utf-8")).get("features", [])

    rows: list[dict] = []
    for feature in stations:
        num = feature.get("properties", {}).get("num", "")
        if not num:
            continue
        try:
            with urllib.request.urlopen(FICHE_URL_TEMPLATE.format(num=num), timeout=10) as r:
                text = r.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001 — fiche manquante : station ignorée
            continue
        parsed = parse_fiche_data(text)
        if not all(key in parsed for key in ("lat", "lon", "ensoleillement_h_an")):
            continue
        rows.append(
            {
                "num_poste": num,
                "lat": parsed["lat"],
                "lon": parsed["lon"],
                "ensoleillement_h_an": parsed["ensoleillement_h_an"],
                "temperature_moy_annuelle": parsed.get("temperature_moy_annuelle", ""),
            }
        )
        if len(rows) % 100 == 0:
            logger.info("[climat] %d/%d stations parsées", len(rows), len(stations))

    if not rows:
        raise RuntimeError("climat : aucune station parsée, source indisponible ?")

    def _write_csv(path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)

    if fetch.is_gcs_uri(dest):
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            _write_csv(tmp_path)
            fetch.upload_to_gcs(tmp_path, dest)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    else:
        _write_csv(dest)

    logger.info("[ok] climat : %d stations -> %s", len(rows), dest)
    return dest
