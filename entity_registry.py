"""
entity_registry.py
------------------
Central in-memory key registry.

Role
----
* When a PRIMARY KEY value is generated, it is registered here.
* When a FOREIGN KEY column needs a value, it is randomly sampled
  from this registry — guaranteeing referential integrity.

Design
------
- Uses a dict[table][column] → list[Any] structure.
- Thread-safe via a threading.Lock for parallel generation.
- Supports sampling with or without replacement.
"""

from __future__ import annotations

import logging
import random
import threading
from typing import Any

logger = logging.getLogger("app")


class EntityRegistry:
    """
    Thread-safe central registry for primary key values.

    Usage:
        registry.register("tstg_company", "company_code", ["C001", "C002"])
        val = registry.sample("tstg_company", "company_code")
    """

    def __init__(self, loggers: dict):
        self.log = loggers["app"]
        self._store: dict[str, dict[str, list[Any]]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def register(self, table: str, column: str, values: list[Any]) -> None:
        """
        Bulk-register generated PK values for a (table, column) pair.
        Appends to any existing values.
        """
        with self._lock:
            self._store.setdefault(table, {}).setdefault(column, [])
            self._store[table][column].extend(values)
            total = len(self._store[table][column])
        self.log.debug("Registry: %s.%s → %d values registered (%d total).",
                       table, column, len(values), total)

    # ------------------------------------------------------------------
    # Read / Sample
    # ------------------------------------------------------------------

    def sample(self, table: str, column: str, k: int = 1) -> list[Any]:
        """
        Sample `k` values from the registry for (table, column).
        Sampling is with replacement — a parent row can be FK-referenced
        multiple times by different child rows.

        Raises KeyError if the parent has not been registered yet.
        """
        with self._lock:
            pool = self._store.get(table, {}).get(column)

        if not pool:
            raise KeyError(
                f"EntityRegistry: no values for {table}.{column}. "
                "Ensure parent table is generated before child."
            )

        return random.choices(pool, k=k)

    def sample_one(self, table: str, column: str) -> Any:
        """Convenience: sample a single value."""
        return self.sample(table, column, k=1)[0]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def has(self, table: str, column: str) -> bool:
        """Return True if there are registered values for (table, column)."""
        with self._lock:
            return bool(self._store.get(table, {}).get(column))

    def count(self, table: str, column: str) -> int:
        with self._lock:
            return len(self._store.get(table, {}).get(column, []))

    def tables(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())

    def summary(self) -> str:
        with self._lock:
            lines = ["Entity Registry Summary:"]
            for tbl, cols in sorted(self._store.items()):
                for col, vals in sorted(cols.items()):
                    lines.append(f"  {tbl}.{col:30s}  {len(vals):>10,} values")
        return "\n".join(lines)