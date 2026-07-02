from __future__ import annotations

from pathlib import Path

from duckpipe.datasets.csv import CsvDataset


def test_csv_load(con, tmp_path: Path) -> None:
    csv_path = tmp_path / "input.csv"
    csv_path.write_text("a,b\n1,x\n2,y\n")

    dataset = CsvDataset(str(csv_path))
    table_name = dataset.load(con, table_name="my_table")

    assert table_name == "my_table"
    rows = con.execute("SELECT a, b FROM my_table ORDER BY a").fetchall()
    assert rows == [("1", "x"), ("2", "y")]


def test_csv_save_roundtrip(con, tmp_path: Path) -> None:
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v(a, b)")
    out_path = tmp_path / "subdir" / "output.csv"

    dataset = CsvDataset(str(out_path))
    dataset.save(con, table_name="t")

    assert out_path.exists()
    reloaded = CsvDataset(str(out_path), all_varchar=False)
    reloaded.load(con, table_name="t_reloaded")
    rows = con.execute("SELECT a, b FROM t_reloaded ORDER BY a").fetchall()
    assert rows == [(1, "x"), (2, "y")]


def test_csv_exists(tmp_path: Path) -> None:
    csv_path = tmp_path / "missing.csv"
    assert CsvDataset(str(csv_path)).exists(None) is False

    csv_path.write_text("a\n1\n")
    assert CsvDataset(str(csv_path)).exists(None) is True
