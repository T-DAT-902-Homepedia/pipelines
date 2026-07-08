"""CLI de l'étape NLP des avis.

    python -m homepedia_ville_ideale.nlp build    --csv data/avis_top80.csv --silver-root ../duckpipe/data/silver [--no-model] [--bronze-copy gs://...]
    python -m homepedia_ville_ideale.nlp calibrate --csv data/avis_top80.csv [--no-model]

``build`` produit les 3 Parquet silver ; ``calibrate`` affiche la corrélation
sentiment/note (métrique de validation). ``--no-model`` désactive le
transformer (signal structurel seul) : utile hors-ligne et en test.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from homepedia_ville_ideale.nlp import runner
from homepedia_ville_ideale.nlp.sentiment import NullBackend, SentimentBackend, TransformersBackend

logger = logging.getLogger(__name__)


def _make_backend(no_model: bool) -> SentimentBackend:
    if no_model:
        logger.info("mode --no-model : sentiment structurel seul (pas de transformer)")
        return NullBackend()
    logger.info("chargement du backend transformer…")
    return TransformersBackend()


def _copy_bronze(csv_path: Path, bronze_uri: str) -> None:
    """Copie le CSV brut vers le bronze (lignage) : local ou GCS."""
    if bronze_uri.startswith("gs://"):
        from google.cloud import storage  # noqa: PLC0415 — extra "gcs"

        bucket_name, _, blob_path = bronze_uri.removeprefix("gs://").partition("/")
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(
            str(csv_path), content_type="text/csv"
        )
    else:
        import shutil  # noqa: PLC0415

        dest = Path(bronze_uri)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(csv_path, dest)
    logger.info("[ok] bronze <- %s", bronze_uri)


def cmd_build(args: argparse.Namespace) -> None:
    backend = _make_backend(args.no_model)
    if args.bronze_copy:
        _copy_bronze(Path(args.csv), args.bronze_copy)
    stats = runner.build_nlp_outputs(Path(args.csv), args.silver_root, backend=backend)
    logger.info(
        "[ok] build : %d avis, %d segments, %d tokens, %d communes (modèle=%s)",
        stats.n_avis, stats.n_segments, stats.n_tokens, stats.n_communes, stats.model_name,
    )


def cmd_calibrate(args: argparse.Namespace) -> None:
    backend = _make_backend(args.no_model)
    report = runner.calibrate(Path(args.csv), backend=backend)
    print(json.dumps(report, ensure_ascii=False, indent=2))  # noqa: T201 — sortie CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="homepedia_ville_ideale.nlp")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="CSV avis -> 3 Parquet silver")
    p_build.add_argument("--csv", required=True)
    p_build.add_argument("--silver-root", required=True, help="dossier local ou gs://.../silver")
    p_build.add_argument("--no-model", action="store_true")
    p_build.add_argument("--bronze-copy", help="copie le CSV brut vers ce chemin (lignage)")
    p_build.set_defaults(func=cmd_build)

    p_cal = sub.add_parser("calibrate", help="corrélation sentiment/note")
    p_cal.add_argument("--csv", required=True)
    p_cal.add_argument("--no-model", action="store_true")
    p_cal.set_defaults(func=cmd_calibrate)

    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
