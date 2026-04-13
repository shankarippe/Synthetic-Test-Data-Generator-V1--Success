"""
auto_ratio_inferrer.py
----------------------
Closes Gap 2: "New tables require manual config updates"

Automatically infers child:parent ratios from the FK dependency graph
so that config.yaml ratios: section is ENTIRELY OPTIONAL.

How it works
------------
Strategy 1 — Live DB sampling (best, used when DB is accessible):
    SELECT COUNT(*) from each table in the real database.
    Compute ratio = child_count / parent_count from actual data.
    If the DB is empty (fresh schema), falls back to Strategy 2.

Strategy 2 — Graph-topology heuristics (used when DB is empty):
    Apply banking domain heuristics based on table name patterns:
      - *_details tables       → ratio 1   (1:1 with parent)
      - transaction/stmt/entry → ratio 20  (many per account)
      - account/arrangement    → ratio 3   (few per customer)
      - customer               → ratio 50  (per company)
      - reference/master       → ratio 10  (per company)
      - default                → ratio 5

Strategy 3 — Config override (always wins if present):
    If config.yaml has a ratios: entry for a table, that always wins
    over inferred ratios.

Result
------
Returns a complete volume_plan dict (table → row_count) covering
ALL tables in the schema, including ones not mentioned in config.yaml.
New tables added to PostgreSQL are auto-sized on next run — zero config.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dependency_graph import DependencyGraph
    from db_metadata_reader import TableMeta

logger = logging.getLogger("app")

# ---------------------------------------------------------------------------
# Heuristic ratio rules — ordered, first match wins
# Each entry: (substring_in_table_name, ratio)
# ---------------------------------------------------------------------------
_HEURISTIC_RULES: list[tuple[str, int]] = [
    # Detail / history tables are always 1:1 or 1:N with parent
    ("_details",        1),
    ("_detail",         1),
    ("_history",        3),
    ("_audit",          2),
    ("_log",            5),

    # High-volume transactional tables
    ("stmt_entry",      20),
    ("transaction",     15),
    ("atm_txn",         10),
    ("activity",        8),
    ("entry",           10),

    # Mid-volume operational tables
    ("arrangement",     3),
    ("account",         3),
    ("collateral",      1),
    ("contract",        2),
    ("charge",          4),
    ("payment",         5),
    ("order_entry",     3),

    # Low-volume per-customer tables
    ("customer",        50),
    ("client",          50),

    # Reference/master data tables — small
    ("currency",        10),
    ("country",         20),
    ("sector",          8),
    ("industry",        10),
    ("category",        6),
    ("product",         8),
    ("status",          5),
    ("messagetype",     5),
    ("transaction_code",8),

    # Default fallback
    ("",                5),   # empty string matches everything
]


class AutoRatioInferrer:
    """
    Infers child:parent ratios automatically.
    Merges live DB counts > config overrides > heuristics.
    """

    def __init__(
        self,
        graph: "DependencyGraph",
        table_meta: dict[str, "TableMeta"],
        config: dict,
        loggers: dict,
    ):
        self.graph = graph
        self.table_meta = table_meta
        self.config = config
        self.log = loggers["app"]
        self.audit = loggers["audit"]
        self.db_cfg = config.get("database", {})

        # Config-specified ratios (always override inferred)
        self._config_ratios: dict[str, dict] = config.get("ratios", {})
        self._anchor_entities: dict[str, int] = config.get("anchor_entities", {})

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def infer_volume_plan(self) -> dict[str, int]:
        """
        Return a complete table → row_count mapping for all tables.
        Auto-detects new tables. No manual config required.
        """
        # Step 1: Try to get live counts from DB
        live_counts = self._fetch_live_counts()

        # Step 2: Identify root tables
        root_tables = set(self.graph.root_tables())

        # Step 3: Walk in topological order and assign volumes
        plan: dict[str, int] = {}
        order = self.graph.generation_order()

        for table in order:
            if table in root_tables:
                plan[table] = self._root_volume(table, live_counts)
            else:
                plan[table] = self._child_volume(table, plan, live_counts)

        self._log_plan(plan, live_counts)
        return plan

    def detect_new_tables(self) -> list[str]:
        """
        Return tables present in the schema but absent from config ratios
        AND absent from anchor_entities. These are newly added tables
        that will be auto-sized.
        """
        configured = set(self._config_ratios.keys()) | set(self._anchor_entities.keys())
        all_tables = set(self.table_meta.keys())
        new_tables = all_tables - configured
        if new_tables:
            self.log.info(
                "Auto-sizing %d table(s) not in config (new/unconfigured): %s",
                len(new_tables), sorted(new_tables),
            )
        return sorted(new_tables)

    # ------------------------------------------------------------------
    # Root table volume
    # ------------------------------------------------------------------

    def _root_volume(self, table: str, live_counts: dict[str, int]) -> int:
        # 1. Config anchor wins
        if table in self._anchor_entities:
            return self._anchor_entities[table]

        # 2. Live DB count (if non-empty)
        if live_counts.get(table, 0) > 0:
            count = live_counts[table]
            self.log.info("  Root '%s': live DB count = %d", table, count)
            return count

        # 3. Fallback default for unconfigured root tables
        default = 50
        self.log.warning(
            "  Root table '%s' not in anchor_entities and DB is empty — defaulting to %d rows.",
            table, default,
        )
        return default

    # ------------------------------------------------------------------
    # Child table volume
    # ------------------------------------------------------------------

    def _child_volume(
        self, table: str, plan: dict[str, int], live_counts: dict[str, int]
    ) -> int:
        # 1. Config ratio wins
        if table in self._config_ratios:
            cfg = self._config_ratios[table]
            parent: str = cfg.get("parent", "")
            ratio: int = cfg.get("ratio", 5)
            parent_rows = plan.get(parent, 0) if parent else 0
            count = max(1, int(parent_rows * ratio))
            self.log.debug("  Child '%s': config ratio %d × parent '%s'(%d) = %d",
                           table, ratio, parent, parent_rows, count)
            return count

        # 2. Live DB ratio (if parent also has live data)
        parents = self.graph.parents_of(table)
        if parents and live_counts.get(table, 0) > 0:
            first_parent = parents[0]
            parent_live = live_counts.get(first_parent, 0)
            if parent_live > 0:
                live_ratio = max(1, live_counts[table] // parent_live)
                parent_rows = plan.get(first_parent, 0)
                count = max(1, parent_rows * live_ratio)
                self.log.info(
                    "  Child '%s': live ratio %d (from DB %d/%d) × planned parent %d = %d",
                    table, live_ratio, live_counts[table], parent_live, parent_rows, count,
                )
                return count

        # 3. Heuristic ratio
        ratio = self._heuristic_ratio(table)
        if parents:
            first_parent = parents[0]
            parent_rows = plan.get(first_parent, 0)
            count = max(1, int(parent_rows * ratio))
            self.log.debug(
                "  Child '%s': heuristic ratio %d × parent '%s'(%d) = %d  [AUTO]",
                table, ratio, first_parent, parent_rows, count,
            )
            return count

        # 4. No parents found (orphan) — use anchor default
        return self._anchor_entities.get(table, 50)

    # ------------------------------------------------------------------
    # Heuristic ratio lookup
    # ------------------------------------------------------------------

    def _heuristic_ratio(self, table: str) -> int:
        tbl_lower = table.lower()
        for substring, ratio in _HEURISTIC_RULES:
            if substring in tbl_lower:
                return ratio
        return 5  # hard fallback

    # ------------------------------------------------------------------
    # Live DB count fetch
    # ------------------------------------------------------------------

    def _fetch_live_counts(self) -> dict[str, int]:
        """
        Try to connect and fetch row counts for all tables.
        Returns empty dict if DB is unreachable or tables are empty.
        """
        counts: dict[str, int] = {}
        schema = self.db_cfg.get("schema", "public")

        try:
            import psycopg
            dsn = (
                f"host={self.db_cfg['host']} port={self.db_cfg['port']} "
                f"dbname={self.db_cfg['dbname']} user={self.db_cfg['user']} "
                f"password={self.db_cfg['password']}"
            )
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    # Use pg_stat_user_tables for fast approximate counts
                    cur.execute(
                        """
                        SELECT relname, n_live_tup
                        FROM pg_stat_user_tables
                        WHERE schemaname = %s
                        """,
                        (schema,),
                    )
                    for table_name, approx_count in cur.fetchall():
                        counts[table_name] = int(approx_count or 0)

            non_empty = {k: v for k, v in counts.items() if v > 0}
            if non_empty:
                self.log.info(
                    "Live DB row counts fetched: %d tables have data.", len(non_empty)
                )
            else:
                self.log.info("DB tables are empty — using heuristic ratios.")

        except Exception as exc:
            self.log.warning("Could not fetch live DB counts (%s) — using heuristics.", exc)

        return counts

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def _log_plan(self, plan: dict[str, int], live_counts: dict[str, int]) -> None:
        total = sum(plan.values())
        self.audit.info("=== Auto-Inferred Volume Plan ===")
        for tbl in self.graph.generation_order():
            cnt = plan.get(tbl, 0)
            source = (
                "anchor_config" if tbl in self._anchor_entities
                else "ratio_config" if tbl in self._config_ratios
                else "live_db" if live_counts.get(tbl, 0) > 0
                else "AUTO_HEURISTIC"
            )
            self.audit.info("  %-45s %8d rows  [%s]", tbl, cnt, source)
        self.audit.info("  %-45s %8d rows  TOTAL", "", total)
        self.log.info(
            "Volume plan: %d tables, %d total rows (%d auto-inferred).",
            len(plan), total,
            sum(1 for t in plan if t not in self._anchor_entities and t not in self._config_ratios),
        )