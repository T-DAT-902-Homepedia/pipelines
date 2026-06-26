"""Connexion PostGIS du pipeline gold transport (psycopg, mêmes env que DVF).

Variables d'environnement :
  POSTGRES_HOST (défaut localhost) / POSTGRES_PORT (5432) /
  POSTGRES_DB (homepedia) / POSTGRES_USER (homepedia) / POSTGRES_PASSWORD.
"""

from __future__ import annotations

import os

import psycopg


def dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'homepedia')} "
        f"user={os.environ.get('POSTGRES_USER', 'homepedia')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'changeme-postgres')}"
    )


def connect() -> psycopg.Connection:
    return psycopg.connect(dsn())
