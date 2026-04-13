"""
volume_planner.py
-----------------
Determines how many rows to generate for every table.

Strategy
--------
1. Root tables (no FK parents) receive explicit counts from
   config['anchor_entities'].
2. Child tables derive their count from:
     child_rows = parent_rows × ratio
   where ratio is defined in config['ratios'].
3. Tables not listed in either section but present in the schema are
   sized to a safe default (equal to the smallest anchor count).

The planner walks tables in dependency (topological) order so that
parent volumes are resolved before child volumes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dependency_graph import DependencyGraph

logger = logging.getLogger("app")

DEFAULT_ROOT_ROWS = 50          # Fallback if a root table has no anchor entry
DEFAULT_CHILD_RATIO = 3         # Fallback ratio if child has no ratio entry


class VolumePlanner:
    """
    Resolves row counts for every table in topological order.
    """

    def __init__(
        self,
        graph: "DependencyGraph",
        config: dict,
        loggers: dict,
    ):
        self.graph = graph
        self.anchor_entities: dict[str, int] = config.get("anchor_entities", {})
        self.ratios: dict[str, dict] = config.get("ratios", {})
        self.log = loggers["app"]
        self.audit = loggers["audit"]

        self._plan: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def compute(self) -> dict[str, int]:
        """Return a mapping of table_name → row_count."""
        order = self.graph.generation_order()
        root_tables = set(self.graph.root_tables())

        # Determine the smallest anchor size for default fallback
        fallback_root = (
            min(self.anchor_entities.values()) if self.anchor_entities else DEFAULT_ROOT_ROWS
        )

        for table in order:
            if table in root_tables:
                count = self.anchor_entities.get(table, fallback_root)
                if table not in self.anchor_entities:
                    self.log.warning(
                        "Root table '%s' not in anchor_entities — defaulting to %d rows.",
                        table, count,
                    )
                self._plan[table] = count

            else:
                # Child table — derive from parent
                ratio_cfg = self.ratios.get(table)
                if ratio_cfg:
                    parent_name = ratio_cfg["parent"]
                    ratio = ratio_cfg["ratio"]
                    parent_rows = self._plan.get(parent_name, 0)
                    if parent_rows == 0:
                        self.log.warning(
                            "Parent '%s' of '%s' has 0 rows — using fallback ratio.",
                            parent_name, table,
                        )
                    self._plan[table] = max(1, int(parent_rows * ratio))
                else:
                    # Auto-derive from first resolved parent
                    parents = self.graph.parents_of(table)
                    if parents:
                        first_parent = parents[0]
                        parent_rows = self._plan.get(first_parent, fallback_root)
                        self._plan[table] = max(1, int(parent_rows * DEFAULT_CHILD_RATIO))
                        self.log.warning(
                            "No ratio config for '%s' — auto-derived %d rows "
                            "from parent '%s' × %d.",
                            table, self._plan[table], first_parent, DEFAULT_CHILD_RATIO,
                        )
                    else:
                        # Orphan (no FK parents in graph) — treat as root
                        self._plan[table] = fallback_root
                        self.log.warning(
                            "Table '%s' has no parents and no anchor — defaulting to %d rows.",
                            table, fallback_root,
                        )

        self._audit_plan()
        return self._plan

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _audit_plan(self) -> None:
        total = sum(self._plan.values())
        self.audit.info("=== Volume Plan ===")
        for tbl, cnt in self._plan.items():
            self.audit.info("  %-40s %10d rows", tbl, cnt)
        self.audit.info("  %-40s %10d rows  (TOTAL)", "ALL TABLES", total)
        self.log.info("Volume plan computed: %d tables, %d total rows.", len(self._plan), total)

    def summary_table(self) -> str:
        lines = [f"{'Table':<40} {'Rows':>12}", "-" * 54]
        for tbl, cnt in sorted(self._plan.items(), key=lambda x: -x[1]):
            lines.append(f"{tbl:<40} {cnt:>12,}")
        lines.append("-" * 54)
        lines.append(f"{'TOTAL':<40} {sum(self._plan.values()):>12,}")
        return "\n".join(lines)