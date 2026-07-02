from __future__ import annotations

from pathlib import Path

from duckpipe.catalog import Catalog
from duckpipe.datasets.csv import CsvDataset


def test_catalog_load_save(con, tmp_path: Path) -> None:
    csv_path = tmp_path / "in.csv"
    csv_path.write_text("a\n1\n2\n")
    out_path = tmp_path / "out.csv"

    catalog = Catalog().add("src", CsvDataset(str(csv_path))).add("dst", CsvDataset(str(out_path)))

    table_name = catalog.load(con, "src")
    assert table_name == "src"

    con.execute("CREATE OR REPLACE TABLE dst AS SELECT * FROM src")
    catalog.save(con, "dst")

    assert out_path.exists()
