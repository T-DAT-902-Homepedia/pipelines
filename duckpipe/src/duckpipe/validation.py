"""Étapes validate_silver / validate_gold / publish du DAG.

Ce ne sont pas des transformations table -> table : ce sont des diagnostics
(qui échouent le run si une règle critique est violée) et une copie de
publication. Elles vivent donc comme fonctions appelées par le CLI, pas comme
Node de pipeline (cf. ADR-0003 : les nodes transforment des relations).
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from duckpipe import fetch, quality

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)

GOLD_MIN_COMMUNES = 15_000  # ~17,8k communes fiables attendues ; en dessous, run suspect
KENDALL_TAU_MIN = 0.8
TOP_N = 25


class CriticalValidationError(RuntimeError):
    """Au moins une règle critique est violée : le run doit échouer."""


def _write_report(report: dict, dest: str) -> None:
    content = json.dumps(report, ensure_ascii=False, indent=2)
    if fetch.is_gcs_uri(dest):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            Path(tmp_path).write_text(content, encoding="utf-8")
            fetch.upload_to_gcs(tmp_path, dest)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    else:
        Path(dest).parent.mkdir(parents=True, exist_ok=True)
        Path(dest).write_text(content, encoding="utf-8")
    logger.info("[ok] rapport DQ -> %s", dest)


def validate_silver(
    con: duckdb.DuckDBPyConnection, report_dest: str | None = None
) -> dict:
    """Valide toutes les tables silver présentes en base contre leurs règles
    (quality.SILVER_RULES) + couverture géographique vs commune_geom.

    Écrit le rapport JSON si `report_dest` est fourni, puis lève
    CriticalValidationError si une règle critique est violée (le rapport est
    écrit AVANT de lever, pour rester exploitable au débogage).
    """
    report: dict = {"tables": {}, "coverage": []}
    critical_failures: list[str] = []

    for table, rules in quality.SILVER_RULES.items():
        if not quality.table_exists(con, table):
            report["tables"][table] = "absente (étape non exécutée)"
            continue
        results = quality.validate(con, table, rules)
        report["tables"][table] = results
        for result in results:
            if result["statut"] == "KO" and result["critique"]:
                critical_failures.append(f"{table} : {result['regle']}")

    if quality.table_exists(con, "commune_geom"):
        sources = [
            {"table": table, "label": table}
            for table in quality.SILVER_RULES
            if quality.table_exists(con, table)
        ]
        report["coverage"] = quality.coverage(con, sources)

    if report_dest:
        _write_report(report, report_dest)
    if critical_failures:
        raise CriticalValidationError(
            "règles critiques violées : " + " ; ".join(critical_failures)
        )
    return report


def kendall_tau(ranking_a: list[str], ranking_b: list[str]) -> float:
    """τ de Kendall entre deux classements des mêmes éléments (Python pur,
    O(n²) — suffisant pour un Top 25).
    """
    items_b = set(ranking_b)
    common = [item for item in ranking_a if item in items_b]
    if len(common) < 2:  # noqa: PLR2004 — pas de paire comparable
        return 1.0
    position_b = {item: i for i, item in enumerate(ranking_b)}
    concordant = discordant = 0
    for i in range(len(common)):
        for j in range(i + 1, len(common)):
            if (position_b[common[i]] - position_b[common[j]]) < 0:
                concordant += 1
            else:
                discordant += 1
    return (concordant - discordant) / (concordant + discordant)


def validate_gold(
    con: duckdb.DuckDBPyConnection,
    score_table: str = "score_territoire",
    *,
    previous_top: list[str] | None = None,
    report_dest: str | None = None,
) -> dict:
    """Contrôles gold : volumétrie, score non NULL, gap dans [-1, 1], et
    stabilité du Top 25 vs run précédent (τ de Kendall) si fourni.
    """
    n_communes = con.execute(f"SELECT count(*) FROM {score_table}").fetchone()[0]
    n_score_null = con.execute(
        f"SELECT count(*) FROM {score_table} WHERE score_valeur IS NULL"
    ).fetchone()[0]
    n_gap_hors_bornes = con.execute(
        f"SELECT count(*) FROM {score_table} WHERE gap < -1 OR gap > 1"
    ).fetchone()[0]
    current_top = [
        row[0]
        for row in con.execute(
            f"SELECT code_commune FROM {score_table} "
            f"ORDER BY gap_pondere DESC LIMIT {TOP_N}"
        ).fetchall()
    ]

    report: dict = {
        "nb_communes_scorees": n_communes,
        "nb_score_null": n_score_null,
        "nb_gap_hors_bornes": n_gap_hors_bornes,
        "top_25": current_top,
    }

    critical_failures: list[str] = []
    if n_communes < GOLD_MIN_COMMUNES:
        critical_failures.append(
            f"{n_communes} communes scorées (< {GOLD_MIN_COMMUNES} attendues)"
        )
    if n_score_null:
        critical_failures.append(f"{n_score_null} communes avec score NULL")
    if n_gap_hors_bornes:
        critical_failures.append(f"{n_gap_hors_bornes} gaps hors [-1, 1]")
    if previous_top:
        tau = kendall_tau(previous_top, current_top)
        report["kendall_tau_top25"] = round(tau, 4)
        if tau <= KENDALL_TAU_MIN:
            critical_failures.append(
                f"Top {TOP_N} instable vs run précédent (tau={tau:.3f} <= {KENDALL_TAU_MIN})"
            )

    if report_dest:
        _write_report(report, report_dest)
    if critical_failures:
        raise CriticalValidationError(
            "contrôles gold en échec : " + " ; ".join(critical_failures)
        )
    return report


def publish(run_uri: str, latest_uri: str) -> None:
    """Copie le score du run (`.../run_date=X/score.parquet`) vers le chemin
    stable `latest/` lu par l'API. Fonctionne en local (tests) et sur GCS.
    """
    if fetch.is_gcs_uri(run_uri) != fetch.is_gcs_uri(latest_uri):
        raise ValueError("publish : run et latest doivent être sur le même backend")

    if fetch.is_gcs_uri(run_uri):
        # Copie objet à objet côté serveur, sans transiter par la machine.
        from google.cloud import storage  # noqa: PLC0415 — extra "gcs" prod

        client = storage.Client()
        src_bucket_name, _, src_blob = run_uri.removeprefix("gs://").partition("/")
        dst_bucket_name, _, dst_blob = latest_uri.removeprefix("gs://").partition("/")
        src_bucket = client.bucket(src_bucket_name)
        src_bucket.copy_blob(
            src_bucket.blob(src_blob), client.bucket(dst_bucket_name), dst_blob
        )
    else:
        import shutil  # noqa: PLC0415

        Path(latest_uri).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(run_uri, latest_uri)
    logger.info("[ok] publié %s -> %s", run_uri, latest_uri)
