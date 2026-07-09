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

    # Appariement DVF -> référentiel IRIS (informatif) : mesure le décalage de
    # millésimes COG (communes DVF fusionnées/scindées absentes des contours
    # IRIS), la première cause de mutations perdues par iris_prix.
    if quality.table_exists(con, "dvf") and quality.table_exists(con, "iris_geom"):
        report["iris_match"] = quality.match_rate(con, "dvf", "iris_geom")

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


def validate_gold_quartier(
    con: duckdb.DuckDBPyConnection,
    quartier_table: str = "score_quartier",
    *,
    report_dest: str | None = None,
) -> dict:
    """Contrôles gold sur ``score_quartier`` : bornes de n_prix_iris et
    gap_iris, unicité des IRIS, héritage commune complet.

    Pas de seuil de volumétrie strict (la fenêtre poolée dépend des millésimes
    dvf_points disponibles) mais une table vide est critique : l'étape a
    tourné pour rien. Appelé par le CLI seulement si le parquet existe
    (pattern avis) : l'absence de l'étape n'échoue pas le run.
    """
    n = con.execute(f"SELECT count(*) FROM {quartier_table}").fetchone()[0]
    n_dup = con.execute(
        f"SELECT count(*) - count(DISTINCT code_iris) FROM {quartier_table}"
    ).fetchone()[0]
    n_prix_hs = con.execute(
        f"SELECT count(*) FROM {quartier_table} "
        "WHERE n_prix_iris IS NULL OR n_prix_iris < 0 OR n_prix_iris > 1"
    ).fetchone()[0]
    n_gap_hs = con.execute(
        f"SELECT count(*) FROM {quartier_table} WHERE gap_iris < -1 OR gap_iris > 1"
    ).fetchone()[0]
    n_score_null = con.execute(
        f"SELECT count(*) FROM {quartier_table} WHERE score_commune IS NULL"
    ).fetchone()[0]

    report = {
        "nb_iris_scores": n,
        "nb_code_iris_dupliques": n_dup,
        "nb_n_prix_hors_bornes": n_prix_hs,
        "nb_gap_hors_bornes": n_gap_hs,
        "nb_score_commune_null": n_score_null,
    }

    critical_failures: list[str] = []
    if n == 0:
        critical_failures.append("aucun IRIS scoré (table vide)")
    if n_dup:
        critical_failures.append(f"{n_dup} code_iris dupliqués")
    if n_prix_hs:
        critical_failures.append(f"{n_prix_hs} n_prix_iris hors [0, 1]")
    if n_gap_hs:
        critical_failures.append(f"{n_gap_hs} gap_iris hors [-1, 1]")
    if n_score_null:
        critical_failures.append(f"{n_score_null} IRIS sans score commune hérité")

    if report_dest:
        _write_report(report, report_dest)
    if critical_failures:
        raise CriticalValidationError(
            "contrôles gold quartier en échec : " + " ; ".join(critical_failures)
        )
    return report


def validate_gold_avis(
    con: duckdb.DuckDBPyConnection,
    avis_table: str = "avis_commune",
    *,
    report_dest: str | None = None,
) -> dict:
    """Contrôles gold sur ``avis_commune`` : bornes du sentiment, cohérence du
    flag low_data, n_avis positif, nuage non vide quand il y a des avis.

    Volontairement plus souple que le score (pas de seuil de volumétrie : la
    couverture avis est partielle par nature). Appelé dans un try/except par le
    CLI : l'absence de la table n'échoue pas le run (étape NLP non produite).
    """
    n = con.execute(f"SELECT count(*) FROM {avis_table}").fetchone()[0]
    n_sent_hs = con.execute(
        f"SELECT count(*) FROM {avis_table} "
        "WHERE sentiment_global IS NOT NULL "
        "AND (sentiment_global < -1 OR sentiment_global > 1)"
    ).fetchone()[0]
    n_avis_bad = con.execute(
        f"SELECT count(*) FROM {avis_table} WHERE n_avis <= 0"
    ).fetchone()[0]
    n_low_incoherent = con.execute(
        f"SELECT count(*) FROM {avis_table} WHERE low_data <> (n_avis < 10)"
    ).fetchone()[0]
    n_wordcloud_vide = con.execute(
        f"SELECT count(*) FROM {avis_table} WHERE n_avis > 0 AND len(wordcloud) = 0"
    ).fetchone()[0]

    report = {
        "nb_communes_avis": n,
        "nb_sentiment_hors_bornes": n_sent_hs,
        "nb_n_avis_non_positif": n_avis_bad,
        "nb_low_data_incoherent": n_low_incoherent,
        "nb_wordcloud_vide": n_wordcloud_vide,
    }

    critical_failures: list[str] = []
    if n_sent_hs:
        critical_failures.append(f"{n_sent_hs} sentiments hors [-1, 1]")
    if n_avis_bad:
        critical_failures.append(f"{n_avis_bad} communes avec n_avis <= 0")
    if n_low_incoherent:
        critical_failures.append(f"{n_low_incoherent} flags low_data incohérents")

    if report_dest:
        _write_report(report, report_dest)
    if critical_failures:
        raise CriticalValidationError(
            "contrôles gold avis en échec : " + " ; ".join(critical_failures)
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
