"""CLI duckpipe — point d'entrée unique des tâches du DAG.

Chaque étape du workflow (Cloud Workflows -> Cloud Run Job, cf. ADR-0009)
exécute cette CLI avec des arguments différents :

    python -m duckpipe ingest <source|dvf|climat|dpe|all> [--year] [--env] [--force]
    python -m duckpipe run <pipeline|prix_millesime> [--year] [--env]
    python -m duckpipe validate-silver [--env] [--run-date]
    python -m duckpipe validate-gold [--env] [--run-date]
    python -m duckpipe publish [--env] [--run-date]

`--env local --local-root <dir>` permet de rejouer n'importe quelle étape sur
un poste de dev sans GCS.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

from duckpipe import catalogs, fetch, quality, sources, validation
from duckpipe.connection import get_connection
from duckpipe.fetch_climat import CLIMAT_BRONZE_PATH, build_stations_csv
from duckpipe.fetch_dpe import DPE_BRONZE_PATH, build_dpe_sample
from duckpipe.pipeline_registry import register_pipelines
from duckpipe.pipelines.prix_millesime import make_prix_millesime_pipeline

logger = logging.getLogger(__name__)

DEFAULT_YEAR = 2024


def _today() -> str:
    return datetime.date.today().isoformat()


def cmd_ingest(args: argparse.Namespace) -> None:
    env = catalogs.get_environment(args.env, local_root=args.local_root)
    bronze = env.bronze_root

    names: list[str]
    if args.source == "all":
        names = ["dvf", *sources.SOURCES.keys(), "climat", "dpe"]
    else:
        names = [args.source]

    for name in names:
        if name == "dvf":
            sources.ingest_source(sources.dvf_source(args.year), bronze, force=args.force)
        elif name == "climat":
            build_stations_csv(f"{bronze}/{CLIMAT_BRONZE_PATH}", force=args.force)
        elif name == "dpe":
            build_dpe_sample(f"{bronze}/{DPE_BRONZE_PATH}", force=args.force)
        elif name in sources.SOURCES:
            sources.ingest_source(sources.SOURCES[name], bronze, force=args.force)
        else:
            raise SystemExit(f"source inconnue : {name!r}")
        logger.info("[ok] ingest %s", name)


def cmd_run(args: argparse.Namespace) -> None:
    env = catalogs.get_environment(args.env, local_root=args.local_root)
    catalog = catalogs.build_catalog(env, year=args.year, run_date=args.run_date)

    if args.pipeline == "prix_millesime":
        pipeline = make_prix_millesime_pipeline(args.year)
    else:
        pipelines = register_pipelines()
        if args.pipeline not in pipelines:
            raise SystemExit(
                f"pipeline inconnu : {args.pipeline!r} "
                f"(disponibles : {', '.join(sorted(pipelines))}, prix_millesime)"
            )
        pipeline = pipelines[args.pipeline]

    con = get_connection()
    try:
        pipeline.run(con, catalog)
    finally:
        con.close()
    logger.info("[ok] pipeline %s", args.pipeline)


def cmd_validate_silver(args: argparse.Namespace) -> None:
    env = catalogs.get_environment(args.env, local_root=args.local_root)
    catalog = catalogs.build_catalog(env, year=args.year, run_date=args.run_date)

    con = get_connection()
    try:
        # Matérialise les tables silver présentes pour les passer aux règles.
        for table in [*quality.SILVER_RULES, "commune_geom"]:
            try:
                catalog.load(con, table)
            except Exception:  # noqa: BLE001 — table absente : signalée par le rapport
                logger.warning("[warn] silver %s illisible (étape non exécutée ?)", table)
        validation.validate_silver(
            con, report_dest=catalogs.dq_report_path(env, "silver", args.run_date)
        )
    finally:
        con.close()
    logger.info("[ok] validate_silver")


def cmd_validate_gold(args: argparse.Namespace) -> None:
    env = catalogs.get_environment(args.env, local_root=args.local_root)

    con = get_connection()
    try:
        # Les chemins gs:// doivent transiter par le tunnel (ADR-0011) : un
        # read_parquet('gs://...') direct échouerait en 403, DuckDB n'ayant
        # pas d'accès OAuth natif à GCS.
        score_uri = catalogs.gold_score_path(env, args.run_date)
        with fetch.local_read_path(score_uri) as score_path:
            con.execute(
                "CREATE TABLE score_territoire AS SELECT * FROM "
                f"read_parquet('{score_path}')"
            )
        previous_top: list[str] | None = None
        latest = catalogs.gold_latest_path(env)
        latest_exists = (
            fetch.gcs_exists(latest) if fetch.is_gcs_uri(latest) else Path(latest).exists()
        )
        if latest_exists:
            with fetch.local_read_path(latest) as latest_path:
                previous_top = [
                    row[0]
                    for row in con.execute(
                        f"SELECT code_commune FROM read_parquet('{latest_path}') "
                        f"ORDER BY gap_pondere DESC LIMIT {validation.TOP_N}"
                    ).fetchall()
                ]
        validation.validate_gold(
            con,
            previous_top=previous_top,
            report_dest=catalogs.dq_report_path(env, "gold", args.run_date),
        )
    finally:
        con.close()
    logger.info("[ok] validate_gold")


def cmd_publish(args: argparse.Namespace) -> None:
    env = catalogs.get_environment(args.env, local_root=args.local_root)
    validation.publish(
        catalogs.gold_score_path(env, args.run_date), catalogs.gold_latest_path(env)
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="duckpipe")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--env", choices=["local", "prod"], default="local")
        sub.add_argument("--local-root", default="data")
        sub.add_argument("--year", type=int, default=DEFAULT_YEAR)
        sub.add_argument("--run-date", default=_today())

    sub_ingest = subparsers.add_parser("ingest", help="télécharge une source vers le bronze")
    sub_ingest.add_argument("source")
    sub_ingest.add_argument("--force", action="store_true")
    add_common(sub_ingest)
    sub_ingest.set_defaults(func=cmd_ingest)

    sub_run = subparsers.add_parser("run", help="exécute un pipeline du registry")
    sub_run.add_argument("pipeline")
    add_common(sub_run)
    sub_run.set_defaults(func=cmd_run)

    sub_vs = subparsers.add_parser("validate-silver", help="règles DQ sur les tables silver")
    add_common(sub_vs)
    sub_vs.set_defaults(func=cmd_validate_silver)

    sub_vg = subparsers.add_parser("validate-gold", help="contrôles gold + stabilité Top 25")
    add_common(sub_vg)
    sub_vg.set_defaults(func=cmd_validate_gold)

    sub_pub = subparsers.add_parser("publish", help="copie le score du run vers latest/")
    add_common(sub_pub)
    sub_pub.set_defaults(func=cmd_publish)

    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
