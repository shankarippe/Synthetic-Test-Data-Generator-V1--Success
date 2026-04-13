"""
data_generator.py
-----------------
Generates synthetic rows for a single table by inspecting its metadata
and applying type-appropriate AND domain-aware generators.

Design: Fully Data-Driven — Zero Hardcoded Domain Values
---------------------------------------------------------
All domain knowledge (currencies, countries, status codes, product types,
etc.) lives in domains.yaml. This file contains NO hardcoded lists.

To switch domains (e.g. from core_banking to fertilizers):
    Edit domains.yaml → change domain_profile: fertilizers
    Re-run main.py — no Python code changes required.

Value resolution priority (highest to lowest):
    1. table_prefix_overrides  → table-specific column value lists
    2. column_patterns         → exact column name match
    3. suffix_patterns         → column name ends with suffix
    4. substring_patterns      → column name contains substring
    5. smart_type_dispatch     → PostgreSQL type + column name heuristics
       (amounts, rates, dates are handled here with sensible ranges)
    6. faker_fallback           → Faker text for unrecognised varchar/text

Generation mechanics:
    PRIMARY KEY → unique sequential or UUID value
    FOREIGN KEY → sample from EntityRegistry (referential integrity)
    Deferred FK (cycle-broken) → NULL
    Nullable column → NULL with probability null_probability
    Everything else → domain lookup → type dispatch → faker fallback
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Generator, TYPE_CHECKING

import yaml
from faker import Faker

if TYPE_CHECKING:
    from db_metadata_reader import TableMeta, ColumnMeta
    from entity_registry import EntityRegistry
    from dependency_graph import DependencyGraph

logger = logging.getLogger("app")

# ---------------------------------------------------------------------------
# Date/time bounds for banking-era data
# ---------------------------------------------------------------------------
_DATE_START = date(2010, 1, 1)
_DATE_END = date.today()
_DATE_RANGE_DAYS = (_DATE_END - _DATE_START).days

_TS_START = datetime(2010, 1, 1)
_TS_END = datetime.now()
_TS_RANGE_SEC = int((_TS_END - _TS_START).total_seconds())


def _rand_date(start: date = _DATE_START, end: date = _DATE_END) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 1)))


def _rand_ts() -> datetime:
    return _TS_START + timedelta(seconds=random.randint(0, _TS_RANGE_SEC))


# ---------------------------------------------------------------------------
# Domain loader — reads domains.yaml once, shared across all generators
# ---------------------------------------------------------------------------

class DomainConfig:
    """
    Loads and exposes the active domain's pattern dictionaries.
    Instantiated once and injected into every DataGenerator.
    """

    def __init__(self, domains_path: str | Path = "domains.yaml"):
        path = Path(domains_path)
        if not path.exists():
            logger.warning("domains.yaml not found at %s — using pure type dispatch.", path)
            self._active: dict = {}
            return

        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        profile = raw.get("domain_profile", "")
        all_domains = raw.get("domains", {})

        if profile not in all_domains:
            logger.warning(
                "domain_profile '%s' not found in domains.yaml — using pure type dispatch.",
                profile,
            )
            self._active = {}
        else:
            self._active = all_domains[profile]
            logger.info("Domain loaded: '%s' from %s", profile, path)

        self._profile = profile

    # Accessors — all return dicts/None, never raise

    @property
    def column_patterns(self) -> dict[str, list | None]:
        return self._active.get("column_patterns", {})

    @property
    def suffix_patterns(self) -> dict[str, list | None]:
        return self._active.get("suffix_patterns", {})

    @property
    def substring_patterns(self) -> dict[str, list | None]:
        return self._active.get("substring_patterns", {})

    @property
    def table_prefix_overrides(self) -> dict[str, dict]:
        return self._active.get("table_prefix_overrides", {})

    @property
    def profile(self) -> str:
        return self._profile

    def lookup(self, table: str, col_name: str) -> list | None:
        """
        Resolve a value list for (table, col_name) using priority order:
          1. table_prefix_overrides (table-specific)
          2. column_patterns (exact)
          3. suffix_patterns (ends-with)
          4. substring_patterns (contains)
          Returns None if no match → caller falls through to type dispatch.
        """
        col_lower = col_name.lower()

        # 1. Table-specific overrides (highest priority)
        for prefix, overrides in self.table_prefix_overrides.items():
            if table.lower().startswith(prefix.lower()) or table.lower() == prefix.lower():
                if col_lower in {k.lower() for k in overrides}:
                    # Find case-insensitive
                    for k, v in overrides.items():
                        if k.lower() == col_lower:
                            return v if v else None

        # 2. Exact column name match
        for pattern_col, values in self.column_patterns.items():
            if col_lower == pattern_col.lower():
                return values if values else None

        # 3. Suffix match
        for suffix, values in self.suffix_patterns.items():
            if col_lower.endswith(suffix.lower()):
                return values if values else None

        # 4. Substring match
        for substr, values in self.substring_patterns.items():
            if substr.lower() in col_lower:
                return values if values else None

        return None  # No domain match → type dispatch


# ---------------------------------------------------------------------------
# DataGenerator
# ---------------------------------------------------------------------------

class DataGenerator:
    """
    Generates batched synthetic rows for a single table.
    Domain values come entirely from DomainConfig (domains.yaml).
    No domain knowledge is hardcoded here.
    """

    def __init__(
        self,
        table_meta: "TableMeta",
        graph: "DependencyGraph",
        registry: "EntityRegistry",
        domain: DomainConfig,
        config: dict,
        loggers: dict,
    ):
        self.tm = table_meta
        self.graph = graph
        self.registry = registry
        self.domain = domain
        self.batch_size: int = config.get("generation", {}).get("batch_size", 10_000)
        self.null_prob: float = config.get("generation", {}).get("null_probability", 0.05)
        self.log = loggers["app"]
        self.err_log = loggers["error"]

        # Per-table seed from SeedManager (falls back to global seed)
        table_seeds = config.get("generation", {}).get("_table_seeds", {})
        global_seed = config.get("generation", {}).get("seed", 42)
        seed = table_seeds.get(self.tm.name, global_seed)
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        self.log.debug(
            "DataGenerator: table='%s' seed=%d (profile=%s)",
            self.tm.name, seed,
            config.get("generation", {}).get("_seed_profile", "default"),
        )

        self._pk_cols: set[str] = set(self.tm.primary_keys)

        # Only active (non-deferred) FK columns
        self._fk_map: dict[str, tuple[str, str]] = {}
        for fk in self.tm.foreign_keys:
            if not self.graph.is_deferred_fk(self.tm.name, fk.column):
                self._fk_map[fk.column] = (fk.ref_table, fk.ref_column)

        self._pk_counter: dict[str, int] = {pk: 0 for pk in self._pk_cols}

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def generate(self, total_rows: int) -> Generator[list[dict], None, None]:
        """Yield batches; register PKs into EntityRegistry after each batch."""
        generated = 0
        while generated < total_rows:
            batch_count = min(self.batch_size, total_rows - generated)
            batch = self._generate_batch(batch_count)

            for pk_col in self._pk_cols:
                pk_vals = [row[pk_col] for row in batch]
                self.registry.register(self.tm.name, pk_col, pk_vals)

            generated += batch_count
            self.log.debug("  %s: %d / %d rows generated", self.tm.name, generated, total_rows)
            yield batch

    # ------------------------------------------------------------------
    # Batch
    # ------------------------------------------------------------------

    def _generate_batch(self, n: int) -> list[dict]:
        # Pre-sample FK values in bulk (faster than per-row sampling)
        fk_samples: dict[str, list[Any]] = {}
        for col_name, (ref_tbl, ref_col) in self._fk_map.items():
            try:
                fk_samples[col_name] = self.registry.sample(ref_tbl, ref_col, k=n)
            except KeyError as exc:
                self.err_log.error(str(exc))
                raise

        rows = []
        for i in range(n):
            row: dict[str, Any] = {}
            for col in self.tm.columns:
                col_name = col.name

                # Deferred FK (cycle-broken) → NULL
                if self.graph.is_deferred_fk(self.tm.name, col_name):
                    row[col_name] = None

                # Active FK → sample from parent registry
                elif col_name in self._fk_map:
                    row[col_name] = fk_samples[col_name][i]

                # PK → unique value
                elif col_name in self._pk_cols:
                    row[col_name] = self._next_pk(col)

                # Nullable → probabilistic NULL
                elif col.is_nullable and random.random() < self.null_prob:
                    row[col_name] = None

                # Regular → domain lookup → type dispatch
                else:
                    row[col_name] = self._resolve_value(col)

            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # PK generation
    # ------------------------------------------------------------------

    def _next_pk(self, col: "ColumnMeta") -> Any:
        dtype = col.udt_name.lower()
        self._pk_counter[col.name] += 1
        counter = self._pk_counter[col.name]

        if "uuid" in dtype:
            return str(uuid.uuid4())
        elif any(t in dtype for t in ("int2", "int4", "int8", "serial", "bigserial")):
            return counter
        elif "varchar" in dtype or "bpchar" in dtype or "text" in dtype:
            max_len = col.character_maximum_length or 20
            prefix = self._pk_prefix()
            return f"{prefix}{counter:08d}"[:max_len]
        elif "numeric" in dtype or "float" in dtype:
            return float(counter)
        else:
            return counter

    def _pk_prefix(self) -> str:
        """Derive a short meaningful prefix from the table name."""
        tbl = self.tm.name.lower()
        mapping = {
            "customer": "CUST",
            "account":  "ACCT",
            "arrangement": "ARRG",
            "transaction": "TXN",
            "collateral": "COLL",
            "company":   "COMP",
            "product":   "PROD",
            "currency":  "CCY",
            "country":   "CTRY",
            "stmt":      "STMT",
            "payment":   "PMT",
        }
        for keyword, prefix in mapping.items():
            if keyword in tbl:
                return prefix
        return self.tm.name[:4].upper().replace("_", "")

    # ------------------------------------------------------------------
    # Value resolution
    # ------------------------------------------------------------------

    def _resolve_value(self, col: "ColumnMeta") -> Any:
        """
        Resolution chain:
          1. Domain pattern lookup (domains.yaml)
          2. Smart heuristics for amounts / rates / dates by column name
          3. PostgreSQL type dispatch
          4. Faker fallback
        """
        # Step 1 — domain lookup
        value_list = self.domain.lookup(self.tm.name, col.name)
        if value_list is not None:
            return random.choice(value_list)

        # Step 2 — smart heuristics by column name keywords
        heuristic = self._heuristic_value(col)
        if heuristic is not None:
            return heuristic

        # Step 3 — PostgreSQL type dispatch
        return self._type_dispatch(col)

    def _heuristic_value(self, col: "ColumnMeta") -> Any | None:
        """
        Apply naming heuristics for common column patterns that should
        produce sensible numeric/date ranges regardless of domain.
        These are structural (not domain-specific) so they stay in code.
        """
        col_lower = col.name.lower()
        dtype = col.udt_name.lower()

        # Dates — any column with 'date' in name
        if "date" in col_lower and "timestamp" not in dtype:
            if "birth" in col_lower or "dob" in col_lower:
                # Date of birth: 18–80 years ago
                start = _DATE_END - timedelta(days=80 * 365)
                end = _DATE_END - timedelta(days=18 * 365)
                return _rand_date(start, end)
            if "expir" in col_lower or "maturity" in col_lower or "end_date" in col_lower:
                # Future date
                return _rand_date(_DATE_END, _DATE_END + timedelta(days=10 * 365))
            return _rand_date()

        # Timestamps
        if "timestamp" in dtype or "date_time" in col_lower:
            return _rand_ts()

        # Amounts / balances — keep in realistic range for numeric columns
        if any(k in col_lower for k in ("amount", "balance", "bal", "_amt", "price", "value")):
            if "numeric" in dtype or "float" in dtype:
                return round(random.uniform(100.0, 5_000_000.0), 2)

        # Rates — small decimal
        if any(k in col_lower for k in ("rate", "spread", "percentage", "percent")):
            if "numeric" in dtype or "float" in dtype:
                return round(random.uniform(0.001, 25.0), 6)

        # Quantities / counts
        if any(k in col_lower for k in ("count", "num_", "number_of", "qty", "quantity")):
            if "numeric" in dtype or "int" in dtype:
                return random.randint(1, 999)

        # Period / term in months/years
        if any(k in col_lower for k in ("period", "tenor", "term", "duration")):
            if "numeric" in dtype or "int" in dtype:
                return random.choice([1, 3, 6, 12, 24, 36, 60, 120])

        # Single char Y/N flags
        if ("bpchar" in dtype or "char" in dtype) and col.character_maximum_length == 1:
            return random.choice(["Y", "N"])

        # m / s columns (T24 multi-value table keys)
        if col_lower in ("m", "s"):
            return random.randint(1, 10)

        # curr_no (T24 version number)
        if col_lower == "curr_no":
            return random.randint(1, 999)

        return None

    # ------------------------------------------------------------------
    # PostgreSQL type dispatch (pure structural, no domain knowledge)
    # ------------------------------------------------------------------

    def _type_dispatch(self, col: "ColumnMeta") -> Any:
        dtype = col.udt_name.lower()

        if "uuid" in dtype:
            return str(uuid.uuid4())

        elif any(t in dtype for t in ("int2", "int4", "int8", "serial", "bigserial")):
            return random.randint(1, 2_147_483_647)

        elif "numeric" in dtype or "float4" in dtype or "float8" in dtype:
            prec = col.numeric_precision or 10
            scale = col.numeric_scale or 2
            # Guard against extreme precision (e.g. numeric(131089,0) in T24)
            if prec > 20 or prec == 0:
                prec = 10
            safe_scale = min(scale, prec - 1)
            max_val = 10 ** max(prec - safe_scale, 1) - 1
            return round(random.uniform(0, min(max_val, 9_999_999)), safe_scale)

        elif "bool" in dtype:
            return random.choice([True, False])

        elif "date" in dtype and "timestamp" not in dtype:
            return _rand_date()

        elif "timestamp" in dtype:
            return _rand_ts()

        elif "time" in dtype:
            return f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"

        elif "json" in dtype:
            return f'{{"key": "{self.fake.word()}", "value": {random.randint(1, 999)}}}'

        elif "text" in dtype:
            return self.fake.sentence(nb_words=random.randint(3, 8))

        elif "varchar" in dtype or "bpchar" in dtype or "char" in dtype:
            max_len = col.character_maximum_length or 50
            if max_len == 1:
                return random.choice(["Y", "N"])
            # Short varchar → word; long varchar → sentence
            if max_len <= 10:
                return self.fake.lexify(text="?" * min(max_len, 6)).upper()
            elif max_len <= 50:
                return self.fake.word()[:max_len]
            else:
                text = self.fake.sentence(nb_words=5).replace(".", "").strip()
                return text[:max_len]

        elif "bytea" in dtype:
            return bytes(random.getrandbits(8) for _ in range(16))

        elif "inet" in dtype:
            return self.fake.ipv4()

        elif "interval" in dtype:
            return f"{random.randint(1, 365)} days"

        else:
            self.log.debug("Unrecognised type '%s' for '%s' — faker fallback.", dtype, col.name)
            return self.fake.lexify(text="??????")

    # ------------------------------------------------------------------
    # Column ordering — must match CSV header
    # ------------------------------------------------------------------

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.tm.columns]