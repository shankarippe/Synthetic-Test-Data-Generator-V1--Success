"""
auto_pipeline.py  [v2 — Multi-DB + T24 Precision + FastAPI ready]
-----------------------------------------------------------------
THE ONLY FILE YOU NEED TO RUN (for CLI mode).

What happens automatically:
  1. Reads your database schema (Postgres / Oracle / SQL Server / MySQL)
  2. Detects your business domain (T24-aware)
  3. Infers T24-precise column values
  4. Infers volume ratios from table relationships
  5. Generates business-relevant test scenarios
  6. Writes domains.yaml, config.yaml, scenarios.yaml automatically
  7. Generates synthetic data and loads back into your database

Usage
-----
  # Full automatic run (PostgreSQL default)
  python auto_pipeline.py

  # Oracle
  python auto_pipeline.py --engine oracle --host localhost --service-name ORCL
                          --user system --password secret

  # SQL Server
  python auto_pipeline.py --engine sqlserver --host localhost --db DatagenDB
                          --user sa --password secret

  # MySQL
  python auto_pipeline.py --engine mysql --host localhost --db DatagenDB
                          --user root --password secret

  # Start FastAPI server
  python auto_pipeline.py --serve

  # Dry run (generate CSVs, skip DB load)
  python auto_pipeline.py --dry-run

Environment variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD  (PostgreSQL)
  GROQ_API_KEY
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import yaml


def main() -> None:
    args = _parse_args()

    # ── FastAPI server mode ────────────────────────────────────────────
    if args.serve:
        _start_server(args)
        return

    print("\n" + "=" * 65)
    print("  Synthetic Data Framework — Fully Automated Pipeline v2")
    print("  Multi-DB: Postgres | Oracle | SQL Server | MySQL")
    print("  Powered by LangGraph + LangChain + Groq (Llama 3.3 70B)")
    print("=" * 65)

    db_config = _resolve_db_config(args)
    engine = db_config.get("engine", "postgres")
    print(f"\n  Engine   : {engine.upper()}")
    print(f"  Database : {db_config.get('host', 'localhost')}:{db_config.get('port', '?')}/{db_config.get('dbname', '?')}")
    print(f"  Schema   : {db_config.get('schema', 'public')}")

    api_key = args.groq_key or os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        print("\n[ERROR] Groq API key required.")
        print("  Option 1: python auto_pipeline.py --groq-key gsk_xxxx")
        print("  Option 2: set GROQ_API_KEY=gsk_xxxx in environment")
        sys.exit(1)

    print(f"\n  LLM      : Groq / {args.model}")
    print("  Starting automated pipeline...\n")

    from Intelligence.llm_client import LLMClient
    from Intelligence.graph import build_graph
    from Intelligence.state import PipelineState

    llm_client = LLMClient(api_key=api_key, model=args.model)

    db_config["_config_path"] = args.config
    db_config["_dry_run"] = args.dry_run

    full_config = {
        "database": db_config,
        "generation": {
            "batch_size": 10000,
            "null_probability": 0.05,
            "output_dir": "./output",
            "seed": args.seed or 42,
        },
        "loader": {"disable_fk_checks": False, "disable_indexes": True},
        "logging": {
            "app_log": "logs/app.log",
            "error_log": "logs/error.log",
            "audit_log": "logs/audit.log",
            "level": "INFO",
        },
    }

    if args.seed_profile:
        full_config["generation"]["seed_profile"] = args.seed_profile

    initial_state = PipelineState(
        db_url=_build_dsn(db_config),
        db_config=full_config,
    )

    graph = build_graph(llm_client)
    t0 = time.perf_counter()

    try:
        final_state = graph.invoke(initial_state)
    except Exception as exc:
        print(f"\n[ERROR] Pipeline failed: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    elapsed = time.perf_counter() - t0

    print("\n" + "=" * 65)
    print("  Pipeline Complete")
    print("=" * 65)
    print(f"  Engine             : {engine.upper()}")
    print(f"  Domain detected    : {final_state.detected_domain}")
    print(f"  Confidence         : {final_state.domain_confidence:.0%}")
    print(f"  T24 precision      : {full_config.get('_is_t24', False)}")
    print(f"  Tables processed   : {len(final_state.table_meta)}")
    print(f"  Rows generated     : {final_state.total_rows_generated:,}")
    print(f"  LLM calls made     : {final_state.llm_calls}")
    print(f"  Scenarios created  : {len(final_state.scenarios)}")
    print(f"  Elapsed            : {elapsed:.1f}s")
    print(f"\n  Files written:")
    print(f"    {final_state.domains_yaml_path}")
    print(f"    {final_state.config_yaml_path}")
    print(f"    {final_state.scenarios_yaml_path}")

    if final_state.errors:
        print(f"\n  Warnings ({len(final_state.errors)}):")
        for err in final_state.errors:
            print(f"    - {err}")

    print("\n  Available scenarios for next run:")
    for name, scenario in final_state.scenarios.items():
        desc = scenario.get("description", "")
        print(f"    python auto_pipeline.py --scenario {name}")
        print(f"      → {desc}")

    print("\n  To expose as REST API:")
    print("    python auto_pipeline.py --serve")
    print("    Then open: http://localhost:8000/docs")
    print("\n" + "=" * 65 + "\n")


# ---------------------------------------------------------------------------
# FastAPI server mode
# ---------------------------------------------------------------------------

def _start_server(args: argparse.Namespace) -> None:
    """Start the FastAPI server."""
    try:
        import uvicorn
    except ImportError:
        print("[ERROR] uvicorn not installed. Run: pip install uvicorn")
        sys.exit(1)

    host = args.api_host or "0.0.0.0"
    port = args.api_port or 8000

    print("\n" + "=" * 65)
    print("  Synthetic Data Generation API Server")
    print("=" * 65)
    print(f"  Starting at: http://{host}:{port}")
    print(f"  Swagger UI : http://{host}:{port}/docs")
    print(f"  ReDoc      : http://{host}:{port}/redoc")
    print("=" * 65 + "\n")

    uvicorn.run(
        "api.main:app",
        host=host,
        port=port,
        reload=args.reload,
        log_level="info",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db_config(args: argparse.Namespace) -> dict:
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path) as fh:
            cfg = yaml.safe_load(fh)
        db = cfg.get("database", {})
        if db.get("host") and db.get("dbname"):
            # Override engine if specified on CLI
            if args.engine:
                db["engine"] = args.engine
            return db

    engine = args.engine or "postgres"
    base = {
        "engine": engine,
        "host": args.host or os.environ.get("PGHOST", "localhost"),
        "dbname": args.db or os.environ.get("PGDATABASE", ""),
        "user": args.user or os.environ.get("PGUSER", ""),
        "password": args.password or os.environ.get("PGPASSWORD", ""),
        "schema": args.schema or "public",
    }
    # Engine-specific port defaults
    port_defaults = {"postgres": 5432, "oracle": 1521, "sqlserver": 1433, "mysql": 3306}
    base["port"] = int(args.port or os.environ.get("PGPORT", port_defaults.get(engine, 5432)))
    if args.service_name:
        base["service_name"] = args.service_name
    return base


def _build_dsn(cfg: dict) -> str:
    engine = cfg.get("engine", "postgres")
    if engine in ("postgres", "postgresql"):
        return (
            f"postgresql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg.get('port', 5432)}/{cfg['dbname']}"
        )
    return f"{engine}://{cfg['user']}@{cfg['host']}/{cfg.get('dbname', '')}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fully automated synthetic data pipeline — multi-DB, zero config"
    )

    # DB connection
    parser.add_argument("--config",    default="config.yaml")
    parser.add_argument("--engine",    default=None,
                        choices=["postgres", "oracle", "sqlserver", "mysql"],
                        help="Database engine (default: postgres or from config.yaml)")
    parser.add_argument("--host",      default=None)
    parser.add_argument("--port",      default=None)
    parser.add_argument("--db",        default=None, help="Database name")
    parser.add_argument("--user",      default=None)
    parser.add_argument("--password",  default=None)
    parser.add_argument("--schema",    default="public")
    parser.add_argument("--service-name", default=None, help="Oracle service name")

    # LLM
    parser.add_argument("--groq-key",  default=None)
    parser.add_argument("--model",     default="llama-3.3-70b-versatile")

    # Run options
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--scenario",  default=None)
    parser.add_argument("--seed",      default=None, type=int)
    parser.add_argument("--seed-profile", default=None)

    # FastAPI server
    parser.add_argument("--serve",     action="store_true",
                        help="Start FastAPI server instead of running pipeline")
    parser.add_argument("--api-host",  default="0.0.0.0")
    parser.add_argument("--api-port",  default=8000, type=int)
    parser.add_argument("--reload",    action="store_true",
                        help="Enable FastAPI hot reload (development mode)")

    return parser.parse_args()


if __name__ == "__main__":
    main()