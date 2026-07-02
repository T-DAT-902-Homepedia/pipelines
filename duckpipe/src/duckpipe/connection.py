from __future__ import annotations

import duckdb


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Ouvre une connexion DuckDB (fichier si `db_path` est fourni, sinon in-memory).

    Charge l'extension `spatial` (ST_Read, ST_Contains, ST_Area_Spheroid).
    L'accès GCS ne passe PAS par DuckDB : l'API S3-compatible exigerait des
    clés HMAC statiques, interdites par la politique d'organisation — les
    Datasets tunnellent les chemins gs:// via google-cloud-storage (OAuth ADC
    en local, metadata server sur Cloud Run), cf. fetch.local_read_path.
    """
    con = duckdb.connect(db_path or ":memory:")
    con.execute("INSTALL spatial; LOAD spatial;")
    # L'extension spatiale (ST_Read, ST_Contains) produit des résultats non
    # déterministes sous exécution multi-thread (observé : un compte de lignes
    # variable d'un run à l'autre sur ST_Read + jointure spatiale, sur des
    # données pourtant identiques). Le volume du pipeline (~1,3 Go) reste
    # largement traitable en mono-thread ; on privilégie la reproductibilité.
    con.execute("SET threads = 1")
    return con
