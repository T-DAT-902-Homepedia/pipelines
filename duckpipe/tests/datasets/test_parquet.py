from __future__ import annotations

from pathlib import Path

from duckpipe.datasets.parquet import ParquetDataset


def test_parquet_roundtrip(con, tmp_path: Path) -> None:
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'x'), (2, 'y')) AS v(a, b)")
    out_path = tmp_path / "silver" / "t.parquet"

    dataset = ParquetDataset(str(out_path))
    dataset.save(con, table_name="t")

    assert out_path.exists()
    dataset.load(con, table_name="t_reloaded")
    rows = con.execute("SELECT a, b FROM t_reloaded ORDER BY a").fetchall()
    assert rows == [(1, "x"), (2, "y")]


def test_parquet_partition_by(con, tmp_path: Path) -> None:
    con.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'a'), (3, 'b')) AS v(id, grp)"
    )
    out_path = tmp_path / "partitioned"

    dataset = ParquetDataset(str(out_path), partition_by=["grp"])
    dataset.save(con, table_name="t")

    assert (out_path / "grp=a").exists()
    assert (out_path / "grp=b").exists()


def test_parquet_exists(tmp_path: Path) -> None:
    parquet_path = tmp_path / "missing.parquet"
    assert ParquetDataset(str(parquet_path)).exists(None) is False
