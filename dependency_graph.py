"""
dependency_graph.py
-------------------
Builds a directed acyclic graph (DAG) of table dependencies based on
foreign-key relationships extracted by DBMetadataReader.

Uses NetworkX for:
  - Directed graph construction (parent → child edges)
  - Root table detection  (tables with no incoming FK edges)
  - Topological sort to determine safe generation order
  - Cycle detection + automatic cycle breaking

Cycle Breaking Strategy
-----------------------
Banking schemas commonly have legitimate circular FK references
(e.g. tstg_currency.country_code <-> tstg_country.currency_code).
When a cycle is detected:
  1. Find the edge in the cycle whose FK column is NULLABLE — it can
     safely be set to NULL during generation and filled in later.
  2. If all columns are NOT NULL, remove the edge with the lexicographically
     smaller constraint name (deterministic tie-break).
  3. Record the broken edge in `self.deferred_fks` so the data generator
     treats that column as nullable/optional.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import networkx as nx

if TYPE_CHECKING:
    from db_metadata_reader import TableMeta

logger = logging.getLogger("app")


class DependencyGraph:
    """
    Wraps a NetworkX DiGraph where an edge A → B means
    "table A must be populated before table B".
    """

    def __init__(self, table_meta: dict[str, "TableMeta"], loggers: dict):
        self.log = loggers["app"]
        # FK edges removed to break cycles; those columns become nullable in generation
        self.deferred_fks: list[dict] = []
        self.graph: nx.DiGraph = nx.DiGraph()
        self._build(table_meta)

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def _build(self, table_meta: dict[str, "TableMeta"]) -> None:
        for tbl_name in table_meta:
            self.graph.add_node(tbl_name)

        for tbl_name, tm in table_meta.items():
            for fk in tm.foreign_keys:
                ref = fk.ref_table
                if ref not in self.graph:
                    self.log.warning(
                        "FK in '%s' references unknown table '%s' — skipping edge.",
                        tbl_name, ref,
                    )
                    continue
                col_meta = tm.column_map.get(fk.column)
                is_nullable = col_meta.is_nullable if col_meta else True
                self.graph.add_edge(
                    ref, tbl_name,
                    fk_column=fk.column,
                    ref_column=fk.ref_column,
                    constraint_name=fk.constraint_name,
                    is_nullable=is_nullable,
                )

        # Auto-resolve cycles — common in banking/core-banking schemas
        self._resolve_cycles()

        self.log.info(
            "Dependency graph built: %d nodes, %d edges",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )

    def _resolve_cycles(self) -> None:
        """Iteratively detect and break cycles until the graph is a DAG."""
        iteration = 0
        while True:
            cycles = list(nx.simple_cycles(self.graph))
            if not cycles:
                break

            iteration += 1
            cycle = cycles[0]
            self.log.warning(
                "Circular FK detected (pass %d): %s — auto-breaking.", iteration, cycle
            )

            edge = self._pick_edge_to_break(cycle)
            if not edge:
                self.log.error("Cannot resolve cycle %s — aborting cycle resolution.", cycle)
                break

            parent, child = edge
            data = self.graph.edges[parent, child]
            self.log.warning(
                "  Deferring FK: %s.%s → %s.%s (constraint: %s). "
                "Column will be NULL during generation.",
                child, data.get("fk_column"),
                parent, data.get("ref_column"),
                data.get("constraint_name"),
            )
            self.deferred_fks.append({
                "child_table": child,
                "parent_table": parent,
                "fk_column": data.get("fk_column"),
                "ref_column": data.get("ref_column"),
                "constraint_name": data.get("constraint_name"),
            })
            self.graph.remove_edge(parent, child)

        if iteration > 0:
            self.log.info("Resolved %d cycle(s) via FK deferral.", iteration)

    def _pick_edge_to_break(self, cycle: list[str]) -> tuple[str, str] | None:
        """
        Pick the best edge to remove from a cycle:
        1. Prefer nullable FK columns (safe to NULL out).
        2. Tie-break by constraint name (lexicographic, deterministic).
        """
        cycle_edges = []
        for i in range(len(cycle)):
            src, dst = cycle[i], cycle[(i + 1) % len(cycle)]
            if self.graph.has_edge(src, dst):
                d = self.graph.edges[src, dst]
                cycle_edges.append((src, dst, d))

        if not cycle_edges:
            return None

        nullable = [(s, d, e) for s, d, e in cycle_edges if e.get("is_nullable", True)]
        candidates = nullable if nullable else cycle_edges
        candidates.sort(key=lambda x: x[2].get("constraint_name", ""))
        return candidates[0][0], candidates[0][1]

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def root_tables(self) -> list[str]:
        roots = [n for n in self.graph.nodes if self.graph.in_degree(n) == 0]
        self.log.debug("Root tables: %s", roots)
        return roots

    def generation_order(self) -> list[str]:
        order = list(nx.topological_sort(self.graph))
        self.log.info("Generation order determined: %d tables", len(order))
        return order

    def parents_of(self, table: str) -> list[str]:
        return list(self.graph.predecessors(table))

    def children_of(self, table: str) -> list[str]:
        return list(self.graph.successors(table))

    def fk_edges_for(self, table: str) -> list[dict]:
        edges = []
        for parent in self.graph.predecessors(table):
            data = self.graph.edges[parent, table]
            edges.append({
                "parent": parent,
                "fk_column": data.get("fk_column"),
                "ref_column": data.get("ref_column"),
            })
        return edges

    def is_deferred_fk(self, table: str, column: str) -> bool:
        """Return True if this FK was removed to break a cycle — treat as nullable."""
        return any(
            d["child_table"] == table and d["fk_column"] == column
            for d in self.deferred_fks
        )

    def summary(self) -> str:
        lines = [
            "Dependency Graph Summary:",
            f"  Tables : {self.graph.number_of_nodes()}",
            f"  Edges  : {self.graph.number_of_edges()}",
        ]
        for node in self.generation_order():
            parents = self.parents_of(node)
            children = self.children_of(node)
            lines.append(f"  {node:40s}  parents={parents}  children={children}")
        if self.deferred_fks:
            lines.append("\n  Deferred FKs (cycle-broken — will be NULL):")
            for d in self.deferred_fks:
                lines.append(
                    f"    {d['child_table']}.{d['fk_column']} → "
                    f"{d['parent_table']}.{d['ref_column']}"
                )
        return "\n".join(lines)