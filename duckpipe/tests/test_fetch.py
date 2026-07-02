"""Tests hors-ligne de la couche fetch : URLs file://, zip locaux, parsing
des fiches climatologiques. Aucun accès réseau ni GCS.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from duckpipe import fetch, sources
from duckpipe.fetch_climat import parse_fiche_data


def test_download_copies_content(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("a,b\n1,2\n")
    dest = tmp_path / "bronze" / "dest.csv"

    result = fetch.download(source.as_uri(), str(dest))

    assert result == str(dest)
    assert dest.read_text() == "a,b\n1,2\n"


def test_download_is_idempotent(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("version originale")
    dest = tmp_path / "dest.csv"

    fetch.download(source.as_uri(), str(dest))
    source.write_text("version modifiée")
    fetch.download(source.as_uri(), str(dest))

    assert dest.read_text() == "version originale"  # pas re-téléchargé

    fetch.download(source.as_uri(), str(dest), force=True)
    assert dest.read_text() == "version modifiée"


def test_extract_from_zip(tmp_path: Path) -> None:
    archive = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("dossier/donnees.csv", "x,y\n3,4\n")
    dest = tmp_path / "extrait.csv"

    result = fetch.extract_from_zip(str(archive), "dossier/donnees.csv", str(dest))

    assert result == str(dest)
    assert dest.read_text() == "x,y\n3,4\n"


def test_extract_from_zip_glob_takes_latest(tmp_path: Path) -> None:
    """Les membres horodatés (ex. Géorisques) sont résolus par glob, le plus
    récent gagnant en cas de versions multiples."""
    archive = tmp_path / "gaspar.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("catnat_gaspar_2025-01-01.csv", "ancien")
        zf.writestr("catnat_gaspar_2026-06-29.csv", "recent")
        zf.writestr("pprn_gaspar_2026-06-29.csv", "autre")
    dest = tmp_path / "catnat.csv"

    fetch.extract_from_zip(str(archive), "catnat_gaspar*.csv", str(dest))

    assert dest.read_text() == "recent"


def test_ingest_source_with_zip_member(tmp_path: Path) -> None:
    archive_source = tmp_path / "gaspar_source.zip"
    with zipfile.ZipFile(archive_source, "w") as zf:
        zf.writestr("catnat.csv", "code_commune;lib\n01001;Inondation\n")

    spec = sources.SourceSpec(
        name="test_zip",
        url=archive_source.as_uri(),
        bronze_path="risques/archive.zip",
        zip_member="catnat.csv",
        zip_member_bronze_path="risques/catnat.csv",
    )
    bronze_root = str(tmp_path / "bronze")

    result = sources.ingest_source(spec, bronze_root)

    assert result == f"{bronze_root}/risques/catnat.csv"
    assert Path(result).read_text() == "code_commune;lib\n01001;Inondation\n"
    assert (Path(bronze_root) / "risques" / "archive.zip").exists()


def test_ingest_source_plain_file(tmp_path: Path) -> None:
    source = tmp_path / "revenus.csv"
    source.write_text("Code;Revenu\n01001;22000\n")
    spec = sources.SourceSpec(
        name="test_plain", url=source.as_uri(), bronze_path="revenus/revenus.csv"
    )
    bronze_root = str(tmp_path / "bronze")

    result = sources.ingest_source(spec, bronze_root)

    assert Path(result).read_text() == "Code;Revenu\n01001;22000\n"


FICHE_EXEMPLE = "\n".join(
    [
        "FICHE CLIMATOLOGIQUE",
        "Statistiques 1991-2020 et records",
        "AMBERIEU (01) Indicatif : 01089001",
        'alt : 250m lat : 45°58\'35"N lon : 05°19\'45"E',
        "",
        "Insolation totale (heures)",
        " 100,0;100,0;100,0;100,0;100,0;100,0;100,0;100,0;100,0;100,0;100,0;100,0;1989,0",
        "",
        "Température moyenne (Moyenne en °C)",
        " 1,0;2,0;3,0;4,0;5,0;6,0;7,0;8,0;9,0;10,0;11,0;12,0;11,9",
    ]
)


# Valeurs attendues pour la fiche d'exemple (station Ambérieu 01089001, mêmes
# valeurs que la première ligne du CSV de référence de exploration/data/raw/).
FICHE_ATTENDUE = {
    "lat": 45.97639,
    "lon": 5.32917,
    "ensoleillement_h_an": 1989.0,
    "temperature_moy_annuelle": 11.9,
}


def test_parse_fiche_data() -> None:
    assert parse_fiche_data(FICHE_EXEMPLE) == FICHE_ATTENDUE
