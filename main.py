"""
main.py
-------
Enterprise Synthetic Data Generation Framework — Orchestrator

Execution flow
--------------
1.  Load config.yaml
2.  Load domains.yaml → DomainConfig (active domain)
3.  Initialise logging (app.log, error.log, audit.log)
4.  Connect to PostgreSQL → extract full schema metadata
5.  Build dependency graph (FK-based DAG, cycles auto-resolved)
6.  Compute volume plan (anchor entities + ratio propagation)
7.  For each table in topological order:
      a. Instantiate DataGenerator (with DomainConfig injected)
      b. Stream batches → FileWriter (CSV)
      c. Register PKs into EntityRegistry
8.  For each table in topological order:
      a. COPY CSV → PostgreSQL via PostgresLoader
9.  Print final audit summary

Usage
-----
    python main.py [--config config.yaml] [--domains domains.yaml]
                   [--dry-run] [--no-load] [--no-confirm]

Flags
-----
    --config <path>    Path to YAML config  (default: config.yaml)
    --domains <path>   Path to domains YAML (default: domains.yaml)
    --dry-run          Generate plan + CSVs but skip DB load
    --no-load          Same as --dry-run
    --no-confirm       Skip interactive confirmation prompt
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml

from logging_setup import setup_logging
from db_metadata_reader import DBMetadataReader
from dependency_graph import DependencyGraph
from volume_planner import VolumePlanner
from entity_registry import EntityRegistry
from data_generator import DataGenerator, DomainConfig
from file_writer import FileWriter
from postgres_loader import PostgresLoader


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path, encoding="utf-8") as fh:
        config = yaml.safe_load(fh)

    # Initialise logging
    loggers = setup_logging(config)
    log = loggers["app"]
    audit = loggers["audit"]

    log.info("=" * 70)
    log.info("Synthetic Data Generation Framework — START")
    log.info("Config : %s", config_path.resolve())

    # Load domain configuration
    domains_path = Path(args.domains)
    domain = DomainConfig(domains_path)
    log.info("Domain : %s  (from %s)", domain.profile, domains_path)

    audit.info("=" * 70)
    audit.info("RUN START | config=%s | domain=%s", config_path, domain.profile)

    t0 = time.perf_counter()

    try:
        _run(config, domain, loggers, args)
    except Exception as exc:
        loggers["error"].exception("Fatal error: %s", exc)
        log.error("Run FAILED: %s", exc)
        audit.info("RUN FAILED: %s", exc)
        sys.exit(1)

    elapsed = time.perf_counter() - t0
    log.info("Run COMPLETE in %.2f seconds.", elapsed)
    audit.info("RUN COMPLETE | elapsed_sec=%.2f", elapsed)


def _run(config: dict, domain: DomainConfig, loggers: dict, args: argparse.Namespace) -> None:
    log = loggers["app"]
    audit = loggers["audit"]

    # Schema metadata
    log.info("--- Phase 1: Metadata Discovery ---")
    reader = DBMetadataReader(config, loggers)
    table_meta = reader.read_all()
    if not table_meta:
        log.warning("No tables found. Exiting.")
        return

    # Dependency graph
    log.info("--- Phase 2: Dependency Graph ---")
    graph = DependencyGraph(table_meta, loggers)
    log.info("\n%s", graph.summary())

    # Volume plan
    log.info("--- Phase 3: Volume Planning ---")
    planner = VolumePlanner(graph, config, loggers)
    volume_plan = planner.compute()
    log.info("\n%s", planner.summary_table())

    if not args.no_confirm and not args.dry_run and not args.no_load:
        _confirm_or_abort(log)

    # Entity Registry
    registry = EntityRegistry(loggers)

    # Generate + write CSV
    log.info("--- Phase 4: Data Generation (domain: %s) ---", domain.profile)
    generation_order = graph.generation_order()
    csv_paths: dict[str, Path] = {}

    for table_name in generation_order:
        tm = table_meta[table_name]
        total_rows = volume_plan.get(table_name, 0)
        if total_rows == 0:
            log.warning("Skipping '%s': 0 rows planned.", table_name)
            continue

        log.info("Generating: %-40s  rows=%d", table_name, total_rows)
        t1 = time.perf_counter()

        gen = DataGenerator(tm, graph, registry, domain, config, loggers)
        writer = FileWriter(table_name, gen.column_names, config, loggers)
        csv_path = writer.write_all(gen.generate(total_rows), total_rows)
        csv_paths[table_name] = csv_path

        elapsed_gen = time.perf_counter() - t1
        rps = total_rows / elapsed_gen if elapsed_gen > 0 else float("inf")
        log.info("  ✓  %-40s  %d rows in %.2fs  (%.0f rows/s)", table_name, total_rows, elapsed_gen, rps)
        audit.info(
            "GEN | table=%-40s rows=%10d elapsed_sec=%.2f rows_per_sec=%.0f",
            table_name, total_rows, elapsed_gen, rps,
        )

    log.info("%s", registry.summary())

    # Load into PostgreSQL
    if args.dry_run or args.no_load:
        log.info("--- Phase 5: DB Load SKIPPED (--dry-run / --no-load) ---")
        log.info("CSV files written to: %s", config.get("generation", {}).get("output_dir", "./output"))
        return

    log.info("--- Phase 5: Loading into PostgreSQL ---")
    loader = PostgresLoader(config, loggers)
    load_plan = [
        (name, csv_paths[name], [c.name for c in table_meta[name].columns])
        for name in generation_order
        if name in csv_paths
    ]
    results = loader.load_all(load_plan)

    # Final summary
    log.info("--- Final Summary ---")
    log.info("%-45s  %12s", "Table", "Rows Loaded")
    log.info("-" * 60)
    for tbl, n in results.items():
        log.info("%-45s  %12s", tbl, f"{n:,}" if n >= 0 else "N/A")
    log.info("-" * 60)
    total = sum(v for v in results.values() if v >= 0)
    log.info("%-45s  %12s", "TOTAL", f"{total:,}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic Data Generation Framework")
    parser.add_argument("--config",  default="config.yaml",  help="Path to config.yaml")
    parser.add_argument("--domains", default="domains.yaml", help="Path to domains.yaml")
    parser.add_argument("--dry-run",    action="store_true", help="Generate CSVs only, skip DB load")
    parser.add_argument("--no-load",    action="store_true", help="Same as --dry-run")
    parser.add_argument("--no-confirm", action="store_true", help="Skip confirmation prompt")
    return parser.parse_args()


def _confirm_or_abort(log) -> None:
    print("\n" + "=" * 60)
    print(" Review the volume plan above.")
    print(" Type 'yes' to proceed, anything else to abort.")
    print("=" * 60)
    try:
        answer = input("Proceed? [yes/no]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        answer = "no"
    if answer != "yes":
        log.info("Aborted by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()