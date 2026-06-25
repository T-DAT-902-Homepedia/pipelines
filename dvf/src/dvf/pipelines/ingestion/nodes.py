"""
Nodes du pipeline d'ingestion (couche Bronze).

Bronze = copie 1:1 typée du CSV brut, matérialisée en Parquet sur MinIO.

Aucune règle métier ici : seulement le typage et la mise en forme pour exploitation
parallèle (le gzip source est non-splittable -> un seul cœur sinon).
"""

import gzip
from pathlib import Path
import httpx
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Racine du projet Kedro (… /pipelines/dvf), dérivée du fichier pour ne pas
# dépendre du CWD. Le chemin reste identique à celui du dataset catalog
# ``geo_dvf_raw`` (data/01_raw/geo_dvf.csv.gz, relatif au project root).
_PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW_CSV_PATH = _PROJECT_ROOT / "data" / "01_raw" / "geo_dvf.csv.gz"
_CHUNK_SIZE = 1 << 20  # 1 Mio
_DOWNLOAD_TIMEOUT = 300.0  # s — borne le hang si le serveur stalle.


def _is_valid_gzip(path: Path) -> bool:
    """Vérifie que le fichier est un gzip lisible (détecte un download tronqué)."""
    try:
        with gzip.open(path, "rb") as f:
            f.read(1)
        return True
    except (OSError, EOFError, gzip.BadGzipFile):
        return False


def ensure_raw_csv(source_url: str) -> str:
    """
    Télécharge le CSV brut Geo DVF s'il est absent/invalide. Renvoie son chemin.

    Idempotent : si le fichier présent est un gzip valide (volume monté, run
    précédent), aucun téléchargement.

    Permet d'exécuter le pipeline depuis un clone vierge sans déposer manuellement le fichier 
    (~94 Mo).

    Le téléchargement passe par un fichier ``.part`` puis un rename atomique : un download interrompu
    ne laisse jamais un ``dest`` corrompu exploitable.
    """
    dest = RAW_CSV_PATH
    if dest.exists() and dest.stat().st_size > 0 and _is_valid_gzip(dest):
        return str(dest)

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    try:
        with httpx.stream("GET", source_url, follow_redirects=True, timeout=_DOWNLOAD_TIMEOUT) as resp:
            resp.raise_for_status()

            with open(tmp, "wb") as f:
                for chunk in resp.iter_bytes(_CHUNK_SIZE):
                    f.write(chunk)

        if not _is_valid_gzip(tmp):
            raise OSError(f"Téléchargement corrompu (gzip invalide) : {source_url}")

        tmp.rename(dest)  # rename atomique : pas de fichier partiel exploité.

    except BaseException:
        tmp.unlink(missing_ok=True)  # pas de .part résiduel après échec.
        raise

    return str(dest)


def to_bronze(geo_dvf_raw: DataFrame, raw_csv_ready: str, repartitions: int) -> DataFrame:
    """
    Type la date et repartitionne pour débloquer le parallélisme aval.

    ``raw_csv_ready`` (chemin du CSV garanti présent par ``ensure_raw_csv``) n'est
    pas utilisé fonctionnellement : il force l'ordre du DAG (téléchargement avant
    lecture du catalog).

    Le DataFrame est chargé avec le schéma explicite (catalog) ;
    on convertit ``date_mutation`` en DateType, le reste est conservé tel quel.
    """
    _ = raw_csv_ready

    return geo_dvf_raw.withColumn(
        "date_mutation", F.to_date("date_mutation", "yyyy-MM-dd")
    ).repartition(repartitions)
