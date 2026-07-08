"""Normalisation SQL des codes INSEE partagée entre pipelines.

Piège DuckDB : ``lpad('971', 2, '0')`` TRONQUE à 2 caractères (sémantique
PostgreSQL) — les départements d'outre-mer (971…976, 3 caractères) étaient
tous écrasés en « 97 », fusionnant leurs agrégats et cassant le rattachement
région. Le padding ne doit s'appliquer qu'aux codes trop courts (Excel-style
« 1 » -> « 01 »), jamais raccourcir.
"""

from __future__ import annotations


def dept_code_expr(column: str) -> str:
    """Expression SQL : code département paddé à 2 caractères minimum,
    sans troncature (couvre 01-95, 2A/2B et l'outre-mer 971-976)."""
    col = f"trim(CAST({column} AS VARCHAR))"
    return f"CASE WHEN length({col}) < 2 THEN lpad({col}, 2, '0') ELSE {col} END"
