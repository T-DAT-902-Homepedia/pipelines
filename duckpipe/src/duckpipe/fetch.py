"""Téléchargement des sources opendata vers le bronze (local ou GCS).

Adaptation de `exploration/src/io.py` (download_cached, extract_from_zip,
datagouv_resource_url) : stdlib uniquement (urllib, pas de requests/tqdm),
et destination polymorphe — un chemin local pour les tests, un URI `gs://`
en prod (upload via google-cloud-storage, dépendance de l'extra "gcs").

Toutes les fonctions sont idempotentes : une destination déjà présente et
non vide n'est pas re-téléchargée (sauf `force=True`).
"""

from __future__ import annotations

import fnmatch
import json
import logging
import shutil
import tempfile
import urllib.request
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60


def is_gcs_uri(path: str) -> bool:
    return str(path).startswith("gs://")


def download(
    url: str,
    dest: str,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    force: bool = False,
) -> str:
    """Télécharge `url` vers `dest` (chemin local ou URI gs://), une seule fois.

    Écriture atomique en local (fichier .part puis rename) : jamais de fichier
    partiel en cache. En cas d'échec réseau l'exception remonte à l'appelant.
    """
    if is_gcs_uri(dest):
        return _download_to_gcs(url, dest, headers=headers, timeout=timeout, force=force)

    dest_path = Path(dest)
    if dest_path.exists() and dest_path.stat().st_size > 0 and not force:
        logger.info("[cache] %s déjà présent", dest_path.name)
        return str(dest_path)

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info("[download] %s -> %s", url, dest_path)
    tmp = dest_path.with_suffix(dest_path.suffix + ".part")
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response, open(tmp, "wb") as fh:
        shutil.copyfileobj(response, fh, length=1 << 16)
    tmp.replace(dest_path)
    return str(dest_path)


def _gcs_blob(gcs_uri: str):
    # Import paresseux volontaire : google-cloud-storage n'est installé que
    # via l'extra "gcs" (prod) — les tests locaux n'en ont pas besoin.
    from google.cloud import storage  # noqa: PLC0415

    bucket_name, _, blob_name = gcs_uri.removeprefix("gs://").partition("/")
    return storage.Client().bucket(bucket_name).blob(blob_name)


def gcs_exists(gcs_uri: str) -> bool:
    return _gcs_blob(gcs_uri).exists()


def upload_to_gcs(local_path: str, gcs_uri: str) -> str:
    logger.info("[upload] %s -> %s", local_path, gcs_uri)
    _gcs_blob(gcs_uri).upload_from_filename(local_path)
    return gcs_uri


def download_from_gcs(gcs_uri: str, local_path: str) -> str:
    logger.info("[download] %s -> %s", gcs_uri, local_path)
    _gcs_blob(gcs_uri).download_to_filename(local_path)
    return local_path


@contextmanager
def local_read_path(path: str) -> Iterator[str]:
    """Chemin local lisible pour `path` : tel quel en local, téléchargé vers un
    fichier temporaire si gs:// (DuckDB n'a pas d'accès OAuth natif à GCS —
    l'API S3-compatible exigerait des clés HMAC statiques, interdites par la
    politique d'organisation ; on tunnelle donc via google-cloud-storage,
    authentifié par ADC en local et par le metadata server sur Cloud Run).
    """
    if not is_gcs_uri(path):
        yield path
        return
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        download_from_gcs(path, tmp_path)
        yield tmp_path
    finally:
        Path(tmp_path).unlink(missing_ok=True)


@contextmanager
def local_write_path(path: str) -> Iterator[str]:
    """Chemin local inscriptible pour `path`, uploadé vers gs:// à la sortie
    si nécessaire (même logique que local_read_path)."""
    if not is_gcs_uri(path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        yield path
        return
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        yield tmp_path
        upload_to_gcs(tmp_path, path)
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def _download_to_gcs(
    url: str, gcs_uri: str, *, headers: dict[str, str] | None, timeout: int, force: bool
) -> str:
    if not force and gcs_exists(gcs_uri):
        logger.info("[cache] %s déjà présent", gcs_uri)
        return gcs_uri

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        download(url, tmp_path, headers=headers, timeout=timeout, force=True)
        upload_to_gcs(tmp_path, gcs_uri)
    finally:
        Path(tmp_path).unlink(missing_ok=True)
    return gcs_uri


def _resolve_member(archive: zipfile.ZipFile, member: str) -> str:
    """Résout un nom de membre exact ou un motif glob dans l'archive.

    Certains producteurs horodatent leurs fichiers (ex. Géorisques publie
    `catnat_gaspar_2026-06-29.csv` là où c'était `catnat_gaspar.csv`) : un
    motif comme `catnat_gaspar*.csv` reste stable. En cas de matches
    multiples, on prend le dernier par ordre lexicographique (= le plus
    récent pour des dates ISO).
    """
    names = archive.namelist()
    if member in names:
        return member
    matches = sorted(fnmatch.filter(names, member))
    if not matches:
        raise KeyError(f"aucun membre ne correspond à {member!r} dans l'archive : {names}")
    return matches[-1]


def extract_from_zip(zip_path: str, member: str, dest: str, *, force: bool = False) -> str:
    """Extrait `member` (nom exact ou motif glob) de l'archive locale
    `zip_path` vers `dest`, idempotent.

    DuckDB ne lit pas un CSV à l'intérieur d'un zip : on matérialise le membre
    une seule fois. `dest` peut être local ou gs:// (extraction locale puis
    upload).
    """
    if is_gcs_uri(dest):
        if not force and gcs_exists(dest):
            logger.info("[cache] %s déjà présent", dest)
            return dest
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            extract_from_zip(zip_path, member, tmp_path, force=True)
            upload_to_gcs(tmp_path, dest)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        return dest

    dest_path = Path(dest)
    if dest_path.exists() and dest_path.stat().st_size > 0 and not force:
        logger.info("[cache] %s déjà extrait", dest_path.name)
        return str(dest_path)

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        resolved = _resolve_member(archive, member)
        with archive.open(resolved) as src, open(dest_path, "wb") as out:
            shutil.copyfileobj(src, out, length=1 << 16)
    logger.info("[ok] extrait %s -> %s", resolved, dest_path.name)
    return str(dest_path)


def datagouv_resource_url(
    dataset_id: str,
    *,
    fmt: str | None = None,
    title_contains: str | None = None,
    timeout: int = 30,
) -> str | None:
    """Résout dynamiquement l'URL d'une ressource d'un dataset data.gouv.

    Les chemins de fichiers opendata changent souvent ; on interroge l'API
    data.gouv pour retrouver la ressource (filtrée par format et/ou fragment
    de titre). Renvoie None si rien ne correspond.
    """
    api = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    try:
        with urllib.request.urlopen(api, timeout=timeout) as response:
            resources = json.loads(response.read().decode("utf-8")).get("resources", [])
    except Exception as exc:  # noqa: BLE001 — l'appelant décide de logguer et poursuivre
        logger.warning("API data.gouv injoignable pour %s : %s", dataset_id, exc)
        return None

    for res in resources:
        if fmt and (res.get("format") or "").lower() != fmt.lower():
            continue
        if title_contains and title_contains.lower() not in (res.get("title") or "").lower():
            continue
        return res.get("url")
    return None
