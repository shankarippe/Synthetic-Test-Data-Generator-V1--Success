"""
distribution_learner.py
-----------------------
Closes Gap 1: "No automatic domain realism"

Connects to a real (source) database, samples column distributions,
and saves them to distribution_cache.json. The DataGenerator then
uses these distributions to produce values that statistically match
real data — same cardinality, same value frequencies, same ranges.

This is exactly what Tonic AI's "statistical synthesis" does.

What is learned per column
--------------------------
  varchar / text  → top-N most frequent values + their weights (frequency sampling)
  numeric         → min, max, mean, stddev → truncated normal distribution
  date/timestamp  → min, max → uniform in real date range
  boolean         → true_ratio → weighted random
  integer         → min, max, percentiles

Usage
-----
    # Step 1: Learn from real DB (run once, or on schedule)
    python distribution_learner.py --config config.yaml --sample-size 10000

    # Step 2: main.py auto-uses the cache if it exists
    python main.py --no-confirm

The cache file (distribution_cache.json) is checked by DataGenerator
before falling back to domains.yaml or type dispatch.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Any

logger = logging.getLogger("app")

_CACHE_FILE = "distribution_cache.json"
_MAX_CATEGORIES = 50      # Max distinct values to store for categorical columns
_DEFAULT_SAMPLE = 5000    # Rows to sample per table


class DistributionLearner:
    """
    Samples a real PostgreSQL database and learns column value distributions.
    Saves results to distribution_cache.json for use by DataGenerator.
    """

    def __init__(self, config: dict, loggers: dict, sample_size: int = _DEFAULT_SAMPLE):
        self.db_cfg = config["database"]
        self.schema = self.db_cfg.get("schema", "public")
        self.sample_size = sample_size
        self.log = loggers["app"]
        self.err = loggers["error"]
        self.cache_path = Path(config.get("generation", {}).get("output_dir", ".")) / _CACHE_FILE

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def learn_all(self, table_names: list[str]) -> dict:
        """
        Learn distributions for all specified tables.
        Returns the full cache dict and writes it to disk.
        """
        cache: dict[str, dict] = {}
        try:
            import psycopg
            dsn = (
                f"host={self.db_cfg['host']} port={self.db_cfg['port']} "
                f"dbname={self.db_cfg['dbname']} user={self.db_cfg['user']} "
                f"password={self.db_cfg['password']}"
            )
            with psycopg.connect(dsn) as conn:
                for table in table_names:
                    self.log.info("Learning distributions: %s", table)
                    try:
                        cache[table] = self._learn_table(conn, table)
                    except Exception as exc:
                        self.err.exception("Failed to learn '%s': %s", table, exc)

        except Exception as exc:
            self.err.exception("Distribution learning failed: %s", exc)
            return {}

        self._save(cache)
        self.log.info(
            "Distribution cache saved: %s (%d tables)", self.cache_path, len(cache)
        )
        return cache

    # ------------------------------------------------------------------
    # Per-table learning
    # ------------------------------------------------------------------

    def _learn_table(self, conn, table: str) -> dict:
        """Sample the table and learn per-column distributions."""
        qualified = f'"{self.schema}"."{table}"'
        col_dists: dict[str, dict] = {}

        # Get column types first
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, udt_name, is_nullable, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (self.schema, table),
            )
            columns = cur.fetchall()

        if not columns:
            return {}

        # Get actual row count
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {qualified}")
            total_rows = cur.fetchone()[0]

        if total_rows == 0:
            self.log.warning("  Table '%s' is empty — skipping distribution learning.", table)
            return {}

        sample_n = min(self.sample_size, total_rows)
        col_names = [c[0] for c in columns]
        col_sql = ", ".join(f'"{c}"' for c in col_names)

        # Tablesample is faster for large tables
        if total_rows > self.sample_size * 2:
            pct = max(0.1, round((sample_n / total_rows) * 100, 2))
            sample_sql = f"SELECT {col_sql} FROM {qualified} TABLESAMPLE SYSTEM({pct})"
        else:
            sample_sql = f"SELECT {col_sql} FROM {qualified}"

        with conn.cursor() as cur:
            cur.execute(sample_sql)
            rows = cur.fetchall()

        self.log.info("  Sampled %d rows from '%s' (total: %d)", len(rows), table, total_rows)

        # Learn per column
        for col_idx, (col_name, udt_name, is_nullable, max_len, num_prec, num_scale) in enumerate(columns):
            values = [row[col_idx] for row in rows if row[col_idx] is not None]
            null_count = len(rows) - len(values)
            null_rate = null_count / len(rows) if rows else 0

            if not values:
                col_dists[col_name] = {"type": "null_only", "null_rate": 1.0}
                continue

            dist = self._learn_column(values, udt_name.lower(), null_rate, max_len)
            col_dists[col_name] = dist

        return col_dists

    def _learn_column(
        self, values: list, udt: str, null_rate: float, max_len: int | None
    ) -> dict:
        """Learn distribution for a single column."""
        base = {"null_rate": round(null_rate, 4)}

        # Numeric columns
        if any(t in udt for t in ("int2", "int4", "int8", "numeric", "float4", "float8")):
            try:
                nums = [float(v) for v in values if v is not None]
                if not nums:
                    return {**base, "type": "numeric", "min": 0, "max": 1, "mean": 0.5, "stddev": 0.1}
                mean = sum(nums) / len(nums)
                variance = sum((x - mean) ** 2 for x in nums) / len(nums)
                stddev = variance ** 0.5
                return {
                    **base,
                    "type": "numeric",
                    "min": round(min(nums), 6),
                    "max": round(max(nums), 6),
                    "mean": round(mean, 6),
                    "stddev": round(stddev, 6),
                    "p25": round(sorted(nums)[len(nums) // 4], 6),
                    "p75": round(sorted(nums)[3 * len(nums) // 4], 6),
                }
            except Exception:
                pass

        # Boolean
        if "bool" in udt:
            true_count = sum(1 for v in values if v)
            return {**base, "type": "boolean", "true_ratio": round(true_count / len(values), 4)}

        # Date / timestamp
        if "date" in udt or "timestamp" in udt:
            try:
                str_vals = [str(v)[:10] for v in values if v]
                str_vals.sort()
                return {
                    **base,
                    "type": "date",
                    "min": str_vals[0],
                    "max": str_vals[-1],
                }
            except Exception:
                pass

        # Categorical: varchar, char, text — learn top-N value frequencies
        str_values = [str(v) for v in values if v is not None]
        counter = Counter(str_values)
        total = len(str_values)
        cardinality = len(counter)

        if cardinality <= _MAX_CATEGORIES:
            # Store all distinct values with weights
            top_values = [(v, c / total) for v, c in counter.most_common(_MAX_CATEGORIES)]
            return {
                **base,
                "type": "categorical",
                "cardinality": cardinality,
                "values": [v for v, _ in top_values],
                "weights": [round(w, 6) for _, w in top_values],
            }
        else:
            # High cardinality — store top-N and note it's open-ended
            top_values = [(v, c / total) for v, c in counter.most_common(_MAX_CATEGORIES)]
            return {
                **base,
                "type": "high_cardinality",
                "cardinality": cardinality,
                "sample_values": [v for v, _ in top_values],
                "avg_length": round(sum(len(v) for v in str_values) / len(str_values), 1),
                "max_length": max_len,
            }

    # ------------------------------------------------------------------
    # Cache I/O
    # ------------------------------------------------------------------

    def _save(self, cache: dict) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as fh:
            json.dump(cache, fh, indent=2, default=str)

    @staticmethod
    def load_cache(cache_path: str | Path = _CACHE_FILE) -> dict:
        path = Path(cache_path)
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)


# ---------------------------------------------------------------------------
# Distribution-aware value sampler
# Used by DataGenerator to sample from learned distributions
# ---------------------------------------------------------------------------

class DistributionSampler:
    """
    Given a learned distribution dict for a column, generate realistic values.
    Injected into DataGenerator when a cache is available.
    """

    def __init__(self, cache: dict):
        self._cache = cache   # table → col → dist_dict

    def has(self, table: str, col: str) -> bool:
        return table in self._cache and col in self._cache[table]

    def sample(self, table: str, col: str) -> Any:
        dist = self._cache.get(table, {}).get(col)
        if dist is None:
            return None

        dtype = dist.get("type", "")

        if dtype == "categorical":
            values = dist["values"]
            weights = dist["weights"]
            return self._weighted_choice(values, weights)

        elif dtype == "high_cardinality":
            # Use sample_values pool — realistic enough for testing
            return self._weighted_choice(dist["sample_values"], None)

        elif dtype == "numeric":
            import random
            mean = dist.get("mean", 0)
            stddev = dist.get("stddev", 1) or 1
            lo = dist.get("min", mean - 3 * stddev)
            hi = dist.get("max", mean + 3 * stddev)
            # Sample from truncated normal
            for _ in range(10):
                val = random.gauss(mean, stddev)
                if lo <= val <= hi:
                    return round(val, 4)
            return round(random.uniform(lo, hi), 4)

        elif dtype == "boolean":
            import random
            return random.random() < dist.get("true_ratio", 0.5)

        elif dtype == "date":
            import random
            from datetime import date, timedelta
            try:
                start = date.fromisoformat(dist["min"][:10])
                end = date.fromisoformat(dist["max"][:10])
                delta = (end - start).days
                return start + timedelta(days=random.randint(0, max(delta, 1)))
            except Exception:
                return date.today()

        return None

    @staticmethod
    def _weighted_choice(values: list, weights: list | None) -> Any:
        import random
        if not values:
            return None
        if weights:
            return random.choices(values, weights=weights, k=1)[0]
        return random.choice(values)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import yaml

    parser = argparse.ArgumentParser(description="Learn column distributions from real DB")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--sample-size", type=int, default=_DEFAULT_SAMPLE,
                        help=f"Rows to sample per table (default: {_DEFAULT_SAMPLE})")
    parser.add_argument("--tables", nargs="*",
                        help="Specific tables to learn (default: all tables in schema)")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    # Simple console logger for CLI use
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    loggers = {
        "app": logging.getLogger("app"),
        "error": logging.getLogger("error"),
        "audit": logging.getLogger("audit"),
    }

    from db_metadata_reader import DBMetadataReader
    reader = DBMetadataReader(config, loggers)
    table_meta = reader.read_all()

    tables_to_learn = args.tables if args.tables else list(table_meta.keys())
    print(f"\nLearning distributions for {len(tables_to_learn)} tables "
          f"(sample size: {args.sample_size} rows each)...\n")

    learner = DistributionLearner(config, loggers, sample_size=args.sample_size)
    cache = learner.learn_all(tables_to_learn)

    print(f"\nDone. Cache written to: {learner.cache_path}")
    print(f"Tables learned: {len(cache)}")
    non_empty = {t: c for t, c in cache.items() if c}
    print(f"Tables with data: {len(non_empty)}")


if __name__ == "__main__":
    main()