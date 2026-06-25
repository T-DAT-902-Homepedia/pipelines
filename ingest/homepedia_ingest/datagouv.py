"""Client minimal pour l'API data.gouv.fr et téléchargement de resources.

Utilise uniquement la stdlib (urllib) pour éviter une dépendance réseau
supplémentaire. Le téléchargement est streamé sur disque et calcule le SHA-256
du fichier source au passage (traçabilité de la couche bronze).
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

API_RESOURCE_URL = "https://www.data.gouv.fr/api/2/datasets/resources/{resource_id}/"
_USER_AGENT = "Homepedia-bronze-ingest/1.0 (+https://github.com/T-DAT-902-Homepedia)"
_HTTP_TIMEOUT = 300


@dataclass(frozen=True)
class ResourceMeta:
    """Métadonnées d'une resource data.gouv.fr."""

    resource_id: str
    dataset_id: str
    title: str
    url: str
    format: str
    filesize: int | None
    checksum_type: str | None
    checksum_value: str | None


@dataclass(frozen=True)
class DownloadResult:
    """Résultat d'un téléchargement : fichier local, taille et empreinte."""

    path: Path
    size_bytes: int
    sha256: str


def _http_get_json(url: str) -> dict:
    req = urllib.request.Request(
        url, headers={"User-Agent": _USER_AGENT, "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
        return json.load(resp)


def get_resource_meta(resource_id: str) -> ResourceMeta:
    """Résout l'URL et les métadonnées d'une resource via l'API data.gouv.fr."""
    payload = _http_get_json(API_RESOURCE_URL.format(resource_id=resource_id))
    resource = payload.get("resource", payload)
    checksum = resource.get("checksum") or {}
    url = resource.get("url")
    if not url:
        raise ValueError(f"Aucune URL trouvée pour la resource {resource_id}")
    return ResourceMeta(
        resource_id=resource_id,
        dataset_id=payload.get("dataset_id", ""),
        title=resource.get("title", ""),
        url=url,
        format=(resource.get("format") or "").lower(),
        filesize=resource.get("filesize"),
        checksum_type=checksum.get("type"),
        checksum_value=checksum.get("value"),
    )


def download(url: str, dest_dir: Path, *, chunk_size: int = 1 << 20) -> DownloadResult:
    """Télécharge `url` dans `dest_dir` en streaming, renvoie taille + SHA-256.

    L'écriture passe par un fichier temporaire puis un rename atomique pour
    éviter un fichier partiel en cas d'interruption.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.rstrip("/").split("/")[-1] or "download"
    dest = dest_dir / filename
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    sha = hashlib.sha256()
    size = 0
    with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT) as resp:
        with tempfile.NamedTemporaryFile(dir=dest_dir, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            while chunk := resp.read(chunk_size):
                tmp.write(chunk)
                sha.update(chunk)
                size += len(chunk)
    tmp_path.replace(dest)
    return DownloadResult(path=dest, size_bytes=size, sha256=sha.hexdigest())
