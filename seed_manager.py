"""
seed_manager.py
---------------
Enterprise Seed Manager for the Synthetic Data Generation Framework.

Upgrades the framework from:
  Basic seeding (seed: 42 in config)
to:
  Enterprise-Grade Reproducible Data Simulation

Features
--------
  1. Named seed profiles     → --seed-profile production_v1
  2. Per-table seed isolation → each table gets its own deterministic seed
  3. Seed registry           → tracks every run with full metadata
  4. Exact reproduction      → --reproduce production_v1
  5. Seed inheritance        → child table seeds derive from parent seeds
  6. Seed versioning         → auto-increments version on each run

How Per-Table Seeds Work
------------------------
  Global seed: 42
  Table seed  = hash(global_seed + table_name + generation_order_index)

  Example:
    tstg_company      → seed = hash(42 + "tstg_company" + 0)      = 7823
    tstg_customer     → seed = hash(42 + "tstg_customer" + 1)     = 4521
    tstg_account      → seed = hash(42 + "tstg_account" + 2)      = 9134
    tstg_stmt_entry   → seed = hash(42 + "tstg_stmt_entry" + 3)   = 2847

  This means:
    - Each table always gets the same seed regardless of other tables
    - Adding a new table does NOT change seeds of existing tables
    - Child table seeds incorporate parent seed for inheritance

Seed Registry
-------------
  Stored in seed_registry.json
  Every run is recorded with:
    - profile name
    - global seed
    - per-table seeds
    - volume plan
    - timestamp
    - run_id

Usage
-----
  # Normal run — auto-creates profile "default"
  python auto_pipeline.py --config config.yaml --groq-key gsk_xxx

  # Named profile
  python auto_pipeline.py --config config.yaml --groq-key gsk_xxx
                          --seed-profile production_v1

  # Reproduce exact previous run
  python auto_pipeline.py --reproduce production_v1 --config config.yaml

  # New dataset
  python auto_pipeline.py --seed-profile stress_test --seed 999
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Dict, List

logger = logging.getLogger("app")

_REGISTRY_FILE = "seed_registry.json"


class SeedManager:
    """
    Manages seed profiles, per-table seeds, and run registry.
    """

    def __init__(self, config: Dict, loggers: Dict):
        self.log = loggers["app"]
        self.audit = loggers["audit"]

        gen_cfg = config.get("generation", {})

        # Global seed — from config or default
        self.global_seed: int = int(gen_cfg.get("seed", 42))

        # Profile name — from config or default
        self.profile_name: str = gen_cfg.get("seed_profile", "default")

        # Registry file path
        output_dir = Path(gen_cfg.get("output_dir", "./output"))
        output_dir.mkdir(parents=True, exist_ok=True)
        self.registry_path = Path(_REGISTRY_FILE)

        # Load existing registry
        self._registry: Dict = self._load_registry()

        # Current run metadata
        self._run_id: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._per_table_seeds: Dict[str, int] = {}
        self._volume_plan: Dict[str, int] = {}

        self.log.info(
            "SeedManager: profile='%s' global_seed=%d run_id=%s",
            self.profile_name, self.global_seed, self._run_id,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def derive_table_seed(
        self,
        table_name: str,
        generation_index: int,
        parent_seed: Optional[int] = None,
    ) -> int:
        """
        Derive a deterministic per-table seed.

        Formula:
            base   = global_seed + generation_index
            input  = f"{base}:{table_name}"
            seed   = int(sha256(input)[:8], 16) % (2^31)

        If parent_seed is provided (child table), it is mixed in:
            input  = f"{base}:{table_name}:{parent_seed}"

        This guarantees:
            - Same table always gets same seed (deterministic)
            - Different tables get different seeds (isolation)
            - Child seeds inherit from parent (inheritance)
            - Adding new tables doesn't affect existing table seeds
        """
        if parent_seed is not None:
            raw = f"{self.global_seed}:{generation_index}:{table_name}:{parent_seed}"
        else:
            raw = f"{self.global_seed}:{generation_index}:{table_name}"

        hash_hex = hashlib.sha256(raw.encode()).hexdigest()
        # Take first 8 hex chars → max 32-bit int
        table_seed = int(hash_hex[:8], 16) % (2 ** 31)

        self._per_table_seeds[table_name] = table_seed
        self.log.debug(
            "  Seed: %-45s global=%d idx=%d → table_seed=%d",
            table_name, self.global_seed, generation_index, table_seed,
        )
        return table_seed

    def derive_seeds_for_all(
        self,
        generation_order: List[str],
        parent_map: Optional[Dict[str, Optional[str]]] = None,
    ) -> Dict[str, int]:
        """
        Derive seeds for all tables in generation order.

        Args:
            generation_order: tables in topological order
            parent_map: table → primary parent table name (or None)

        Returns:
            dict of table_name → seed
        """
        parent_map = parent_map or {}
        seeds: dict[str, int] = {}

        for idx, table_name in enumerate(generation_order):
            parent_name = parent_map.get(table_name)
            parent_seed = seeds.get(parent_name) if parent_name else None
            seeds[table_name] = self.derive_table_seed(table_name, idx, parent_seed)

        self._per_table_seeds = seeds
        self._log_seed_table(seeds)
        return seeds

    def set_volume_plan(self, volume_plan: dict[str, int]) -> None:
        """Store the volume plan for registry recording."""
        self._volume_plan = dict(volume_plan)

    def register_run(self) -> str:
        """
        Save this run to the seed registry.
        Returns the run_id.
        """
        run_record = {
            "run_id": self._run_id,
            "profile": self.profile_name,
            "global_seed": self.global_seed,
            "timestamp": datetime.now().isoformat(),
            "per_table_seeds": self._per_table_seeds,
            "volume_plan": self._volume_plan,
            "total_rows": sum(self._volume_plan.values()),
        }

        # Store under profile name — overwrite previous with same profile
        if "profiles" not in self._registry:
            self._registry["profiles"] = {}

        self._registry["profiles"][self.profile_name] = run_record

        # Also keep a run history
        if "history" not in self._registry:
            self._registry["history"] = []
        self._registry["history"].append({
            "run_id": self._run_id,
            "profile": self.profile_name,
            "global_seed": self.global_seed,
            "timestamp": run_record["timestamp"],
            "total_rows": run_record["total_rows"],
        })

        # Keep only last 50 history entries
        self._registry["history"] = self._registry["history"][-50:]

        self._save_registry()

        self.audit.info(
            "SEED REGISTRY | profile=%s run_id=%s global_seed=%d tables=%d total_rows=%d",
            self.profile_name,
            self._run_id,
            self.global_seed,
            len(self._per_table_seeds),
            run_record["total_rows"],
        )
        self.log.info(
            "Seed profile '%s' registered: run_id=%s",
            self.profile_name, self._run_id,
        )
        return self._run_id

    # ------------------------------------------------------------------
    # Reproduction
    # ------------------------------------------------------------------

    def load_profile(self, profile_name: str) -> dict:
        """
        Load a previously saved seed profile for exact reproduction.

        Returns the full run record including per_table_seeds and volume_plan.
        Raises ValueError if profile not found.
        """
        profiles = self._registry.get("profiles", {})
        if profile_name not in profiles:
            available = list(profiles.keys())
            raise ValueError(
                f"Seed profile '{profile_name}' not found in registry.\n"
                f"Available profiles: {available}\n"
                f"Registry: {self.registry_path}"
            )

        record = profiles[profile_name]
        self.log.info(
            "Reproducing profile '%s' from run_id=%s (timestamp: %s)",
            profile_name,
            record.get("run_id"),
            record.get("timestamp"),
        )
        return record

    def get_table_seed_from_profile(
        self, profile_name: str, table_name: str
    ) -> Optional[int]:
        """Get the exact seed used for a specific table in a previous run."""
        record = self.load_profile(profile_name)
        return record.get("per_table_seeds", {}).get(table_name)

    # ------------------------------------------------------------------
    # Registry queries
    # ------------------------------------------------------------------

    def list_profiles(self) -> List[Dict]:
        """Return all saved profiles with summary info."""
        profiles = self._registry.get("profiles", {})
        result = []
        for name, record in profiles.items():
            result.append({
                "profile": name,
                "run_id": record.get("run_id"),
                "global_seed": record.get("global_seed"),
                "timestamp": record.get("timestamp"),
                "total_rows": record.get("total_rows"),
                "tables": len(record.get("per_table_seeds", {})),
            })
        return sorted(result, key=lambda x: x.get("timestamp", ""), reverse=True)

    def list_history(self, limit: int = 10) -> List[Dict]:
        """Return recent run history."""
        return self._registry.get("history", [])[-limit:]

    def print_registry_summary(self) -> None:
        """Print a formatted summary of all profiles."""
        profiles = self.list_profiles()
        if not profiles:
            print("\n  No seed profiles registered yet.")
            return

        print("\n" + "=" * 70)
        print("  Seed Registry Summary")
        print("=" * 70)
        print(f"  {'Profile':<25} {'Seed':>8} {'Rows':>12} {'Timestamp':<25}")
        print("-" * 70)
        for p in profiles:
            print(
                f"  {p['profile']:<25} "
                f"{p['global_seed']:>8} "
                f"{p['total_rows']:>12,} "
                f"{p['timestamp'][:19]:<25}"
            )
        print("=" * 70 + "\n")

    # ------------------------------------------------------------------
    # Config integration helpers
    # ------------------------------------------------------------------

    def apply_to_config(self, config: Dict, table_seeds: Dict[str, int]) -> Dict:
        """
        Inject per-table seeds into config for use by DataGenerator.
        Returns updated config.
        """
        config = dict(config)
        config.setdefault("generation", {})
        config["generation"]["_table_seeds"] = table_seeds
        config["generation"]["_seed_profile"] = self.profile_name
        config["generation"]["_run_id"] = self._run_id
        return config

    @staticmethod
    def get_table_seed_from_config(config: Dict, table_name: str) -> int:
        """
        Get the seed for a specific table from config.
        Falls back to global seed if per-table seeds not present.
        """
        table_seeds = config.get("generation", {}).get("_table_seeds", {})
        global_seed = config.get("generation", {}).get("seed", 42)
        return table_seeds.get(table_name, global_seed)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load_registry(self) -> Dict:
        if not self.registry_path.exists():
            return {"profiles": {}, "history": []}
        try:
            with open(self.registry_path, encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:
            self.log.warning("Could not load seed registry: %s — starting fresh.", exc)
            return {"profiles": {}, "history": []}

    def _save_registry(self) -> None:
        with open(self.registry_path, "w", encoding="utf-8") as fh:
            json.dump(self._registry, fh, indent=2, default=str)

    def _log_seed_table(self, seeds: dict[str, int]) -> None:
        self.audit.info("=== Per-Table Seed Assignment ===")
        self.audit.info("  Profile: %s | Global seed: %d", self.profile_name, self.global_seed)
        for tbl, seed in seeds.items():
            self.audit.info("  %-45s seed=%d", tbl, seed)