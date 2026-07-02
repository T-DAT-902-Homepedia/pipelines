from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

    from duckpipe.catalog import Catalog

NodeFunc = Callable[..., "dict[str, str] | str | None"]
# Signature attendue : func(con: DuckDBPyConnection, **inputs_resolus) -> outputs
# où inputs_resolus mappe chaque nom logique d'input à son nom de table réel,
# et le retour est soit un seul nom de table (si un seul output), soit un dict
# {nom_logique_output: nom_table} si plusieurs.


@dataclass(frozen=True)
class Node:
    """Une unité de transformation relation DuckDB -> relation DuckDB.

    Le Node ne connaît que Catalog (abstraction), jamais une implémentation
    concrète de Dataset (DIP) : il charge ses inputs et sauvegarde ses outputs
    par nom logique, sans savoir s'ils viennent d'un CSV local ou d'un Parquet
    GCS.
    """

    func: NodeFunc
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    name: str | None = None

    def run(self, con: duckdb.DuckDBPyConnection, catalog: Catalog) -> None:
        resolved_inputs = {input_name: catalog.load(con, input_name) for input_name in self.inputs}
        result = self.func(con, **resolved_inputs)

        if len(self.outputs) == 1 and isinstance(result, str):
            result = {self.outputs[0]: result}
        elif result is None:
            result = {}

        for output_name in self.outputs:
            produced_table = result[output_name]
            if produced_table != output_name:
                con.execute(
                    f"ALTER TABLE {produced_table} RENAME TO {output_name}"
                )
            catalog.save(con, output_name)


@dataclass
class Pipeline:
    """Suite ordonnée de Node, exécutée séquentiellement.

    Pas d'auto-résolution de graphe de dépendances : Airflow orchestre déjà
    entre tâches (voir ARCHITECTURE.md) ; à l'intérieur d'une tâche
    PythonOperator, le nombre de nodes est petit et leur ordre trivial à
    écrire à la main. KISS : pas de topological sort maison.
    """

    nodes: list[Node]

    def run(self, con: duckdb.DuckDBPyConnection, catalog: Catalog) -> None:
        for node in self.nodes:
            node.run(con, catalog)

    def __add__(self, other: Pipeline) -> Pipeline:
        return Pipeline(nodes=[*self.nodes, *other.nodes])
