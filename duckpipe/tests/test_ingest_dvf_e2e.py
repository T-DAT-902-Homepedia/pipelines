from __future__ import annotations

from pathlib import Path

from duckpipe.catalog import Catalog
from duckpipe.datasets.csv import CsvDataset
from duckpipe.datasets.memory import MemoryDataset
from duckpipe.datasets.parquet import ParquetDataset
from duckpipe.pipelines.dvf import dvf_pipeline

DVF_HEADER = (
    "id_mutation,date_mutation,nature_mutation,type_local,valeur_fonciere,"
    "surface_reelle_bati,code_commune,nom_commune,code_departement,longitude,latitude"
)

# 6 ventes valides sur la commune 75056 (5 Appartement à prix/m² homogène
# autour de 6000 €/m² + 1 Maison), assez pour dépasser le seuil de fiabilité
# (>= 5 transactions) tout en restant dans le clipping p1-p99 par (département,
# type_local). Une 7e ligne, très aberrante, doit être rejetée par les bornes
# absolues de plausibilité (prix/m² > PRIX_M2_MAX).
DVF_ROWS = [
    "1,2024-01-01,Vente,Appartement,300000,50,75056,Paris,75,2.35,48.86",
    "2,2024-01-02,Vente,Appartement,300000,50,75056,Paris,75,2.35,48.86",
    "3,2024-01-03,Vente,Appartement,300000,50,75056,Paris,75,2.35,48.86",
    "4,2024-01-04,Vente,Appartement,300000,50,75056,Paris,75,2.35,48.86",
    "5,2024-01-05,Vente,Appartement,300000,50,75056,Paris,75,2.35,48.86",
    "6,2024-01-06,Vente,Maison,400000,80,75056,Paris,75,2.35,48.86",
    # rejetée : prix/m2 = 100000/1 = hors bornes plausibles (max 50000)
    "7,2024-01-07,Vente,Appartement,100000,1,75056,Paris,75,2.35,48.86",
]


def test_dvf_pipeline_end_to_end(con, tmp_path: Path) -> None:
    raw_path = tmp_path / "dvf_raw.csv"
    raw_path.write_text(DVF_HEADER + "\n" + "\n".join(DVF_ROWS) + "\n")

    catalog = (
        Catalog()
        .add("dvf_raw", CsvDataset(str(raw_path)))
        .add("dvf", ParquetDataset(str(tmp_path / "silver" / "dvf.parquet")))
        .add("commune_agg", ParquetDataset(str(tmp_path / "silver" / "commune_agg.parquet")))
        .add("commune_agg_type", MemoryDataset())
        .add("dept_agg", MemoryDataset())
    )

    dvf_pipeline.run(con, catalog)

    agg = con.execute(
        "SELECT nb_transactions, fiable FROM commune_agg WHERE code_commune = '75056'"
    ).fetchone()
    assert agg is not None
    nb_transactions, fiable = agg
    valid_rows_count = len(DVF_ROWS) - 1  # une ligne rejetée par les bornes de plausibilité
    assert nb_transactions == valid_rows_count
    assert fiable is True

    assert (tmp_path / "silver" / "dvf.parquet").exists()
    assert (tmp_path / "silver" / "commune_agg.parquet").exists()
