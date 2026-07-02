"""Lecture des datasets parquet silver produits par duckpipe.

Le pipeline duckpipe (`pipelines/duckpipe`) écrit la couche silver en parquet
sous `<silver_dir>/<dataset>/<dataset>.parquet` (cf. `duckpipe/catalogs.py`).
On lit ce fichier tel quel : duckpipe est la source de vérité, PostGIS n'est
qu'un cache de service pour l'API.

Variable d'environnement :
  HOMEPEDIA_SILVER_DIR  racine silver locale
                        (défaut ../duckpipe/data/silver, sortie d'un run local
                        `python -m duckpipe run transport --env local`)
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _silver_dir() -> Path:
    default = Path(__file__).resolve().parents[2] / "duckpipe" / "data" / "silver"
    return Path(os.environ.get("HOMEPEDIA_SILVER_DIR", str(default)))


def read_silver(dataset: str) -> pa.Table:
    """Charge le parquet silver `<silver_dir>/<dataset>/<dataset>.parquet`."""
    path = _silver_dir() / dataset / f"{dataset}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"silver introuvable : {path}. Lancer d'abord "
            "`python -m duckpipe run transport --env local` dans pipelines/duckpipe."
        )
    return pq.read_table(path)
