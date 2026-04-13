"""
preflight_check.py
------------------
Run this BEFORE main.py to catch all common issues upfront.

Checks performed:
  1. Database connectivity
  2. Schema table discovery
  3. PK / FK completeness
  4. Circular FK detection + preview of auto-resolution
  5. Volume plan validation (anchors cover all root tables)
  6. Config completeness
  7. Output directory writability

Usage:
    python preflight_check.py --config config.yaml

Exit codes:
    0 — all checks passed
    1 — one or more checks failed (details printed)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

PASS = "  [PASS]"
WARN = "  [WARN]"
FAIL = "  [FAIL]"


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic Data Framework — Pre-flight Checker")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config)
    print("\n" + "=" * 65)
    print("  Synthetic Data Framework — Pre-flight Check")
    print("=" * 65)

    issues: list[str] = []

    # ------------------------------------------------------------------
    # 1. Config file
    # ------------------------------------------------------------------
    print("\n[1] Config file")
    if not config_path.exists():
        print(f"{FAIL} Config not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)
    print(f"{PASS} Config loaded: {config_path}")

    required_keys = ["database", "anchor_entities"]
    for k in required_keys:
        if k not in config:
            issues.append(f"Config missing required key: '{k}'")
            print(f"{FAIL} Missing key: {k}")
        else:
            print(f"{PASS} Key present: {k}")

    # ------------------------------------------------------------------
    # 2. Output directory
    # ------------------------------------------------------------------
    print("\n[2] Output directory")
    out_dir = Path(config.get("generation", {}).get("output_dir", "./output"))
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        test_file = out_dir / ".write_test"
        test_file.write_text("test")
        test_file.unlink()
        print(f"{PASS} Output directory writable: {out_dir}")
    except Exception as e:
        issues.append(f"Output directory not writable: {e}")
        print(f"{FAIL} {e}")

    # ------------------------------------------------------------------
    # 3. Log directory
    # ------------------------------------------------------------------
    print("\n[3] Log directory")
    log_cfg = config.get("logging", {})
    for log_key in ["app_log", "error_log", "audit_log"]:
        log_path = Path(log_cfg.get(log_key, f"logs/{log_key.replace('_log','')}.log"))
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"{PASS} Log path writable: {log_path}")
        except Exception as e:
            issues.append(f"Log directory not writable: {e}")
            print(f"{FAIL} {e}")

    # ------------------------------------------------------------------
    # 4. Database connectivity
    # ------------------------------------------------------------------
    print("\n[4] Database connectivity")
    try:
        import psycopg
        db = config["database"]
        dsn = (f"host={db['host']} port={db['port']} dbname={db['dbname']} "
               f"user={db['user']} password={db['password']}")
        conn = psycopg.connect(dsn)
        print(f"{PASS} Connected to PostgreSQL: {db['host']}/{db['dbname']}")
        conn.close()
    except ImportError:
        issues.append("psycopg not installed — run: pip install psycopg[binary]")
        print(f"{FAIL} psycopg not installed")
        print("\nCannot continue without database connection. Fix above and re-run.")
        _print_summary(issues)
        sys.exit(1)
    except Exception as e:
        issues.append(f"DB connection failed: {e}")
        print(f"{FAIL} {e}")
        print("\nCannot continue without database connection.")
        _print_summary(issues)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 5. Schema metadata + FK analysis
    # ------------------------------------------------------------------
    print("\n[5] Schema metadata")
    from logging_setup import setup_logging
    from db_metadata_reader import DBMetadataReader
    from dependency_graph import DependencyGraph

    loggers = setup_logging(config)

    try:
        reader = DBMetadataReader(config, loggers)
        table_meta = reader.read_all()
        print(f"{PASS} Tables discovered: {len(table_meta)}")
    except Exception as e:
        issues.append(f"Metadata read failed: {e}")
        print(f"{FAIL} {e}")
        _print_summary(issues)
        sys.exit(1)

    # Tables with no PK
    no_pk = [t for t, m in table_meta.items() if not m.primary_keys]
    if no_pk:
        for t in no_pk:
            print(f"{WARN} No PRIMARY KEY defined: {t}")
    else:
        print(f"{PASS} All tables have PRIMARY KEY defined")

    # FK referencing unknown tables
    for tbl, tm in table_meta.items():
        for fk in tm.foreign_keys:
            if fk.ref_table not in table_meta:
                issues.append(
                    f"Table '{tbl}': FK references unknown table '{fk.ref_table}'"
                )
                print(f"{FAIL} {tbl}.{fk.column} → unknown table '{fk.ref_table}'")

    # ------------------------------------------------------------------
    # 6. Circular FK detection
    # ------------------------------------------------------------------
    print("\n[6] Circular FK detection")
    try:
        import networkx as nx

        g = nx.DiGraph()
        for t in table_meta:
            g.add_node(t)
        for tbl, tm in table_meta.items():
            for fk in tm.foreign_keys:
                if fk.ref_table in table_meta:
                    g.add_edge(fk.ref_table, tbl)

        cycles = list(nx.simple_cycles(g))
        if cycles:
            print(f"{WARN} {len(cycles)} circular FK cycle(s) detected — will be auto-resolved:")
            for c in cycles:
                print(f"        Cycle: {' → '.join(c)} → {c[0]}")
            print(f"       The framework will automatically break these by setting")
            print(f"       one FK column to NULL per cycle (see dependency_graph.py).")
        else:
            print(f"{PASS} No circular FK dependencies")
    except ImportError:
        issues.append("networkx not installed — run: pip install networkx")
        print(f"{FAIL} networkx not installed")

    # ------------------------------------------------------------------
    # 7. Volume plan
    # ------------------------------------------------------------------
    print("\n[7] Volume plan")
    from dependency_graph import DependencyGraph
    from volume_planner import VolumePlanner

    graph = DependencyGraph(table_meta, loggers)
    root_tables = set(graph.root_tables())
    anchors = set(config.get("anchor_entities", {}).keys())
    ratios = set(config.get("ratios", {}).keys())

    # Root tables not in anchors
    missing_anchors = root_tables - anchors
    if missing_anchors:
        for t in sorted(missing_anchors):
            print(f"{WARN} Root table not in anchor_entities (will use default): {t}")
    else:
        print(f"{PASS} All root tables have anchor counts")

    # Anchors referring to non-existent tables
    bad_anchors = anchors - set(table_meta.keys())
    if bad_anchors:
        for t in sorted(bad_anchors):
            issues.append(f"anchor_entities refers to unknown table: {t}")
            print(f"{FAIL} anchor_entities entry for unknown table: {t}")

    planner = VolumePlanner(graph, config, loggers)
    plan = planner.compute()
    total = sum(plan.values())
    print(f"{PASS} Volume plan computed: {len(plan)} tables, {total:,} total rows")
    print(f"\n       Top 10 tables by volume:")
    for tbl, cnt in sorted(plan.items(), key=lambda x: -x[1])[:10]:
        print(f"         {tbl:<45} {cnt:>10,}")

    # ------------------------------------------------------------------
    # 8. Dependency checks
    # ------------------------------------------------------------------
    print("\n[8] Generation order")
    order = graph.generation_order()
    print(f"{PASS} Topological order: {len(order)} tables")
    print(f"       First 5: {order[:5]}")
    print(f"       Last 5:  {order[-5:]}")
    if graph.deferred_fks:
        print(f"{WARN} {len(graph.deferred_fks)} FK(s) deferred (set to NULL) to break cycles:")
        for d in graph.deferred_fks:
            print(f"         {d['child_table']}.{d['fk_column']} → "
                  f"{d['parent_table']}.{d['ref_column']}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _print_summary(issues)
    sys.exit(1 if issues else 0)


def _print_summary(issues: list[str]) -> None:
    print("\n" + "=" * 65)
    if issues:
        print(f"  Pre-flight FAILED — {len(issues)} issue(s) found:")
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")
        print("\n  Fix the above issues before running main.py")
    else:
        print("  Pre-flight PASSED — safe to run: python main.py --no-confirm")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()