from __future__ import annotations

import duckdb
import pytest


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()
