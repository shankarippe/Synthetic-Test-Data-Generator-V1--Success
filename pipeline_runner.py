"""
pipeline_runner.py
------------------
Closes Gap 3: "Limited Automation"

Implements a fully automated pipeline that:
  1. Detects schema changes since last run (new/dropped tables, FK changes)
  2. Auto-infers volumes for new tables (no manual config)
  3. Runs the full generation + load pipeline
  4. Optionally schedules itself to run on a cron interval
  5. Writes a run manifest (JSON) for audit/lineage tracking

This makes the framework behave like Tonic AI's scheduled refresh pipelines.

Usage
-----
  # Single run
  python pipeline_runner.py --config config.yaml

  # Single run with scenario
  python pipeline_runner.py --config config.yaml --scenario loan_default_stress

  # Scheduled run every 6 hours
  python pipeline_runner.py --config config.yaml --schedule 6h

  # Dry run (generate CSVs, skip DB load)
  python pipeline_runner.py --config config.yaml --dry-run

  # List available scenarios
  python pipeline_runner.py --list-scenarios

Schema change detection
-----------------------
  On each run, a schema_snapshot.json is written containing table names,
  column counts, and FK counts. On the next run, the snapshot is compared
  and changes are logged. New tables are auto-sized via AutoRatioInferrer.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import yaml

logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------------
# Manifest writer — audit/lineage record for each pipeline run
# ---------------------------------------------------------------------------

class RunManifest:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._data: dict = {
            "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "status": "RUNNING",
            "scenario": None,
            "domain": None,
            "schema_changes": [],
            "tables": {},
            "total_rows": 0,
            "elapsed_seconds": None,
        }

    def set_scenario(self, name: str) -> None:
        self._data["scenario"] = name

    def set_domain(self, name: str) -> None:
        self._data["domain"] = name

    def add_schema_changes(self, changes: list[str]) -> None:
        self._data["schema_changes"] = changes

    def record_table(self, table: str, rows: int, elapsed: float, source: str) -> None:
        self._data["tables"][table] = {
            "rows": rows,
            "elapsed_sec": round(elapsed, 3),
            "volume_source": source,
        }
        self._data["total_rows"] += rows

    def finish(self, status: str = "SUCCESS") -> None:
        self._data["finished_at"] = datetime.now().isoformat()
        self._data["status"] = status
        self._data["elapsed_seconds"] = round(
            (datetime.fromisoformat(self._data["finished_at"]) -
             datetime.fromisoformat(self._data["started_at"])).total_seconds(), 2
        )
        self._save()

    def _save(self) -> None:
        path = self.output_dir / f"run_manifest_{self._data['run_id']}.json"
        with open(path, "w") as fh:
            json.dump(self._data, fh, indent=2)
        logger.info("Run manifest: %s", path)


# ---------------------------------------------------------------------------
# Schema change detector
# ---------------------------------------------------------------------------

class SchemaChangeDetector:
    def __init__(self, snapshot_path: Path):
        self.snapshot_path = snapshot_path

    def snapshot(self, table_meta: dict) -> dict:
        return {
            table: {
                "columns": len(tm.columns),
                "pk_count": len(tm.primary_keys),
                "fk_count": len(tm.foreign_keys),
            }
            for table, tm in table_meta.items()
        }

    def detect_changes(self, table_meta: dict) -> list[str]:
        """Compare current schema to last snapshot. Return list of change descriptions."""
        current = self.snapshot(table_meta)
        changes: list[str] = []

        if not self.snapshot_path.exists():
            changes.append(f"First run — {len(current)} tables discovered.")
            self._save(current)
            return changes

        with open(self.snapshot_path) as fh:
            previous = json.load(fh)

        # New tables
        new_tables = set(current.keys()) - set(previous.keys())
        for t in sorted(new_tables):
            changes.append(f"NEW TABLE: {t}")

        # Dropped tables
        dropped = set(previous.keys()) - set(current.keys())
        for t in sorted(dropped):
            changes.append(f"DROPPED TABLE: {t}")

        # Column changes
        for t in set(current.keys()) & set(previous.keys()):
            if current[t]["columns"] != previous[t]["columns"]:
                changes.append(
                    f"COLUMN CHANGE: {t}  "
                    f"{previous[t]['columns']} → {current[t]['columns']} columns"
                )
            if current[t]["fk_count"] != previous[t]["fk_count"]:
                changes.append(
                    f"FK CHANGE: {t}  "
                    f"{previous[t]['fk_count']} → {current[t]['fk_count']} FKs"
                )

        if not changes:
            changes.append("No schema changes detected.")

        self._save(current)
        return changes

    def _save(self, snapshot: dict) -> None:
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.snapshot_path, "w") as fh:
            json.dump(snapshot, fh, indent=2)


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

class PipelineRunner:
    """
    Orchestrates the full synthetic data generation pipeline with:
      - Schema change detection
      - Auto volume inference
      - Scenario application
      - Scheduling
      - Run manifests
    """

    def __init__(self, config: dict, args: argparse.Namespace):
        self.config = config
        self.args = args
        self.output_dir = Path(config.get("generation", {}).get("output_dir", "./output"))

        # Setup logging
        from logging_setup import setup_logging
        self.loggers = setup_logging(config)
        self.log = self.loggers["app"]

    def run_once(self) -> bool:
        """Execute one full pipeline run. Returns True on success."""
        manifest = RunManifest(self.output_dir)
        t0 = time.perf_counter()

        try:
            self._execute(manifest)
            manifest.finish("SUCCESS")
            return True
        except Exception as exc:
            self.loggers["error"].exception("Pipeline run failed: %s", exc)
            self.log.error("Pipeline FAILED: %s", exc)
            manifest.finish("FAILED")
            return False

    def run_scheduled(self, interval_seconds: int) -> None:
        """Run pipeline on a schedule until interrupted."""
        self.log.info("Scheduled pipeline: every %d seconds.", interval_seconds)
        run_number = 0
        while True:
            run_number += 1
            self.log.info("=== Scheduled Run #%d ===", run_number)
            success = self.run_once()
            if not success:
                self.log.warning("Run #%d failed — will retry at next interval.", run_number)
            self.log.info("Next run in %d seconds...", interval_seconds)
            try:
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                self.log.info("Scheduler interrupted. Exiting.")
                break

    def _execute(self, manifest: RunManifest) -> None:
        from db_metadata_reader import DBMetadataReader
        from dependency_graph import DependencyGraph
        from auto_ratio_inferrer import AutoRatioInferrer
        from entity_registry import EntityRegistry
        from data_generator import DataGenerator, DomainConfig
        from file_writer import FileWriter
        from postgres_loader import PostgresLoader
        from scenario_engine import ScenarioEngine

        log = self.log

        # ── Phase 1: Schema discovery ─────────────────────────────────
        log.info("--- Phase 1: Schema Discovery ---")
        reader = DBMetadataReader(self.config, self.loggers)
        table_meta = reader.read_all()

        # Detect schema changes
        snapshot_path = self.output_dir / "schema_snapshot.json"
        detector = SchemaChangeDetector(snapshot_path)
        changes = detector.detect_changes(table_meta)
        manifest.add_schema_changes(changes)
        for change in changes:
            log.info("  Schema: %s", change)

        # ── Phase 2: Dependency graph ─────────────────────────────────
        log.info("--- Phase 2: Dependency Graph ---")
        graph = DependencyGraph(table_meta, self.loggers)

        # ── Phase 3: Load domain ──────────────────────────────────────
        log.info("--- Phase 3: Domain Config ---")
        domains_path = Path(getattr(self.args, "domains", "domains.yaml"))
        domain = DomainConfig(domains_path)

        # ── Phase 4: Scenario ─────────────────────────────────────────
        scenario_engine = None
        scenario_name = getattr(self.args, "scenario", None)
        if scenario_name:
            log.info("--- Phase 4: Scenario — %s ---", scenario_name)
            scenario_engine = ScenarioEngine(
                scenario_name,
                scenarios_path=getattr(self.args, "scenarios", "scenarios.yaml"),
                loggers=self.loggers,
            )
            log.info("\n%s", scenario_engine.summary())
            manifest.set_scenario(scenario_name)

            # Override domain if scenario specifies one
            domain_override = scenario_engine.get_domain_override()
            if domain_override and domain_override != domain.profile:
                log.info("Scenario overrides domain: %s → %s", domain.profile, domain_override)
                domain = DomainConfig.__new__(DomainConfig)
                # Reload with overridden profile
                import yaml as _yaml
                with open(domains_path) as fh:
                    raw = _yaml.safe_load(fh)
                raw["domain_profile"] = domain_override
                domain._profile = domain_override
                domain._active = raw.get("domains", {}).get(domain_override, {})

            # Inject scenario column overrides into domain
            scenario_engine.inject_into_domain(domain)

        manifest.set_domain(domain.profile)

        # ── Phase 5: Volume plan ──────────────────────────────────────
        log.info("--- Phase 5: Volume Planning (Auto-Infer) ---")
        inferrer = AutoRatioInferrer(graph, table_meta, self.config, self.loggers)
        new_tables = inferrer.detect_new_tables()
        if new_tables:
            log.info("Auto-sizing new tables: %s", new_tables)

        volume_plan = inferrer.infer_volume_plan()

        # Apply scenario volume overrides
        if scenario_engine and scenario_engine.is_active:
            volume_plan = scenario_engine.apply_volume_overrides(volume_plan)

        log.info("Volume plan: %d tables, %d total rows",
                 len(volume_plan), sum(volume_plan.values()))

        # ── Phase 6: Generate + write CSVs ───────────────────────────
        log.info("--- Phase 6: Data Generation ---")
        registry = EntityRegistry(self.loggers)
        generation_order = graph.generation_order()
        csv_paths: dict[str, Path] = {}

        # Apply date context from scenario if available
        if scenario_engine and scenario_engine.is_active:
            date_ctx = scenario_engine.get_date_context()
            if date_ctx:
                import data_generator as dg_module
                dg_module._DATE_START = date_ctx.get("date_start", dg_module._DATE_START)
                dg_module._DATE_END = date_ctx.get("date_end", dg_module._DATE_END)
                dg_module._DATE_RANGE_DAYS = (dg_module._DATE_END - dg_module._DATE_START).days
                log.info(
                    "Date context: %s → %s",
                    dg_module._DATE_START, dg_module._DATE_END,
                )

        for table_name in generation_order:
            tm = table_meta[table_name]
            total_rows = volume_plan.get(table_name, 0)
            if total_rows == 0:
                continue

            log.info("Generating: %-40s  %d rows", table_name, total_rows)
            t1 = time.perf_counter()

            gen = DataGenerator(tm, graph, registry, domain, self.config, self.loggers)
            writer = FileWriter(table_name, gen.column_names, self.config, self.loggers)
            csv_path = writer.write_all(gen.generate(total_rows), total_rows)
            csv_paths[table_name] = csv_path

            elapsed = time.perf_counter() - t1
            rps = total_rows / elapsed if elapsed > 0 else 0
            log.info("  ✓  %-40s  %d rows  %.0f rows/s", table_name, total_rows, rps)
            manifest.record_table(
                table_name, total_rows, elapsed,
                "scenario" if scenario_engine else "auto_inferred",
            )

        # ── Phase 7: Load ─────────────────────────────────────────────
        dry_run = getattr(self.args, "dry_run", False) or getattr(self.args, "no_load", False)
        if dry_run:
            log.info("--- Phase 7: DB Load SKIPPED (dry-run) ---")
            return

        log.info("--- Phase 7: Loading into PostgreSQL ---")
        loader = PostgresLoader(self.config, self.loggers)
        load_plan = [
            (name, csv_paths[name], [c.name for c in table_meta[name].columns])
            for name in generation_order
            if name in csv_paths
        ]
        results = loader.load_all(load_plan)
        total_loaded = sum(v for v in results.values() if v >= 0)
        log.info("Total rows loaded: %d", total_loaded)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_interval(s: str) -> int:
    """Parse interval string like '6h', '30m', '3600s' into seconds."""
    s = s.strip().lower()
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    elif s.endswith("m"):
        return int(s[:-1]) * 60
    elif s.endswith("s"):
        return int(s[:-1])
    else:
        return int(s)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Synthetic Data Pipeline Runner — fully automated"
    )
    parser.add_argument("--config",    default="config.yaml")
    parser.add_argument("--domains",   default="domains.yaml")
    parser.add_argument("--scenarios", default="scenarios.yaml")
    parser.add_argument("--scenario",  default=None,
                        help="Run a specific business scenario")
    parser.add_argument("--schedule",  default=None,
                        help="Run on schedule, e.g. '6h', '30m', '3600s'")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Generate CSVs but skip DB load")
    parser.add_argument("--no-load",   action="store_true")
    parser.add_argument("--list-scenarios", action="store_true",
                        help="List all available scenarios and exit")
    args = parser.parse_args()

    # List scenarios
    if args.list_scenarios:
        from scenario_engine import ScenarioEngine
        scenarios = ScenarioEngine.list_scenarios(args.scenarios)
        print(f"\nAvailable scenarios in {args.scenarios}:\n")
        for s in scenarios:
            print(f"  {s['name']:<35}  {s['description']}")
        print()
        return

    with open(args.config) as fh:
        config = yaml.safe_load(fh)

    # Setup basic logging for the runner itself
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    runner = PipelineRunner(config, args)

    if args.schedule:
        interval = _parse_interval(args.schedule)
        runner.run_scheduled(interval)
    else:
        success = runner.run_once()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()