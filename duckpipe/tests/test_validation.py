"""Tests hors-ligne de validation.py : tau de Kendall, règles critiques,
publication locale."""

from __future__ import annotations

from pathlib import Path

import pytest

from duckpipe import validation
from duckpipe.validation import (
    CriticalValidationError,
    kendall_tau,
    publish,
    validate_silver,
)


def test_kendall_tau_identical() -> None:
    ranking = ["a", "b", "c", "d"]
    assert kendall_tau(ranking, ranking) == 1.0


def test_kendall_tau_reversed() -> None:
    assert kendall_tau(["a", "b", "c", "d"], ["d", "c", "b", "a"]) == -1.0


def test_kendall_tau_one_swap() -> None:
    # une inversion sur 6 paires : (6-2)/6... non : 5 concordantes, 1 discordante
    tau = kendall_tau(["a", "b", "c", "d"], ["b", "a", "c", "d"])
    assert tau == pytest.approx((5 - 1) / 6)


def test_kendall_tau_disjoint_rankings_is_neutral() -> None:
    assert kendall_tau(["a", "b"], ["x", "y"]) == 1.0  # aucune paire comparable


def test_validate_silver_raises_on_critical_violation(con) -> None:
    # Table revenus avec un revenu hors bornes plausibles (règle critique).
    con.execute(
        "CREATE TABLE revenus AS SELECT * FROM (VALUES "
        "('75056', 30000.0, 20000.0), ('01001', 999999.0, 500.0)"
        ") AS v(code_commune, revenu_median, revenu_q1)"
    )
    with pytest.raises(CriticalValidationError, match="revenus"):
        validate_silver(con)


def test_validate_silver_writes_report_before_raising(con, tmp_path: Path) -> None:
    con.execute(
        "CREATE TABLE revenus AS SELECT '01001' AS code_commune, "
        "999999.0 AS revenu_median, 500.0 AS revenu_q1"
    )
    report_path = tmp_path / "silver.json"
    with pytest.raises(CriticalValidationError):
        validate_silver(con, report_dest=str(report_path))
    assert report_path.exists()  # le rapport reste exploitable au débogage


def test_validate_silver_ok_on_clean_table(con) -> None:
    con.execute(
        "CREATE TABLE revenus AS SELECT '75056' AS code_commune, "
        "30000.0 AS revenu_median, 20000.0 AS revenu_q1"
    )
    report = validate_silver(con)
    assert all(r["statut"] == "OK" for r in report["tables"]["revenus"])


def test_validate_gold_raises_on_low_volume(con) -> None:
    con.execute(
        "CREATE TABLE score_territoire AS SELECT '75056' AS code_commune, "
        "0.5 AS score_valeur, 0.1 AS gap, 0.05 AS gap_pondere"
    )
    with pytest.raises(CriticalValidationError, match="communes scorées"):
        validation.validate_gold(con)


def test_publish_local_copy(tmp_path: Path) -> None:
    run_file = tmp_path / "run_date=2026-07-02" / "score.parquet"
    run_file.parent.mkdir(parents=True)
    run_file.write_bytes(b"contenu parquet factice")
    latest = tmp_path / "latest" / "score.parquet"

    publish(str(run_file), str(latest))

    assert latest.read_bytes() == b"contenu parquet factice"
