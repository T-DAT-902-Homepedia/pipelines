from __future__ import annotations

from pathlib import Path

from duckpipe.catalog import Catalog
from duckpipe.datasets.csv import CsvDataset
from duckpipe.node import Node, Pipeline


def double(con, src: str) -> str:
    con.execute(f"CREATE OR REPLACE TABLE doubled AS SELECT a * 2 AS a FROM {src}")
    return "doubled"


def increment(con, doubled: str) -> str:
    con.execute(f"CREATE OR REPLACE TABLE incremented AS SELECT a + 1 AS a FROM {doubled}")
    return "incremented"


def test_pipeline_runs_nodes_in_order(con, tmp_path: Path) -> None:
    src_path = tmp_path / "src.csv"
    src_path.write_text("a\n1\n2\n")
    out_path = tmp_path / "out.csv"

    catalog = (
        Catalog()
        .add("src", CsvDataset(str(src_path), all_varchar=False))
        .add("doubled", CsvDataset(str(tmp_path / "doubled.csv"), all_varchar=False))
        .add("incremented", CsvDataset(str(out_path), all_varchar=False))
    )

    pipeline = Pipeline(
        nodes=[
            Node(func=double, inputs=["src"], outputs=["doubled"], name="double"),
            Node(func=increment, inputs=["doubled"], outputs=["incremented"], name="increment"),
        ]
    )
    pipeline.run(con, catalog)

    rows = con.execute("SELECT a FROM incremented ORDER BY a").fetchall()
    assert rows == [(3,), (5,)]
    assert out_path.exists()


def test_pipeline_add_concatenates_nodes() -> None:
    node_a = Node(func=double, inputs=["src"], outputs=["doubled"], name="double")
    node_b = Node(func=increment, inputs=["doubled"], outputs=["incremented"], name="increment")

    combined = Pipeline(nodes=[node_a]) + Pipeline(nodes=[node_b])

    assert combined.nodes == [node_a, node_b]
