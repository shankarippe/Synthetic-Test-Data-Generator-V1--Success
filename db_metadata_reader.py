"""
db_metadata_reader.py
---------------------
Connects to PostgreSQL and extracts schema metadata exclusively from
information_schema system catalogs. No manual configuration is required.

Extracted:
  - Tables in the target schema
  - Columns with data types and nullability
  - Primary key constraints
  - Foreign key constraints (column → referenced table.column)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import psycopg

logger = logging.getLogger("app")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnMeta:
    name: str
    data_type: str          # PostgreSQL type name from information_schema
    udt_name: str           # Underlying type (e.g. int4, varchar)
    is_nullable: bool
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    ordinal_position: int


@dataclass
class ForeignKeyMeta:
    constraint_name: str
    column: str             # FK column in this table
    ref_table: str          # Referenced table
    ref_column: str         # Referenced column in the parent table


@dataclass
class TableMeta:
    schema: str
    name: str
    columns: list[ColumnMeta] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKeyMeta] = field(default_factory=list)

    @property
    def column_map(self) -> dict[str, ColumnMeta]:
        return {c.name: c for c in self.columns}


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

class DBMetadataReader:
    """
    Reads all schema metadata from PostgreSQL information_schema views.
    The target schema is set in config['database']['schema'].
    """

    def __init__(self, config: dict, loggers: dict):
        self.db_cfg = config["database"]
        self.schema = self.db_cfg.get("schema", "public")
        self.log = loggers["app"]
        self.err_log = loggers["error"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_all(self) -> dict[str, TableMeta]:
        """Return a dict keyed by table name containing full metadata."""
        with self._connect() as conn:
            tables = self._fetch_tables(conn)
            self.log.info("Discovered %d tables in schema '%s'", len(tables), self.schema)

            columns = self._fetch_columns(conn)
            pks = self._fetch_primary_keys(conn)
            fks = self._fetch_foreign_keys(conn)

        # Assemble TableMeta objects
        meta: dict[str, TableMeta] = {}
        for tbl_name in tables:
            tm = TableMeta(schema=self.schema, name=tbl_name)
            tm.columns = columns.get(tbl_name, [])
            tm.primary_keys = pks.get(tbl_name, [])
            tm.foreign_keys = fks.get(tbl_name, [])
            meta[tbl_name] = tm
            self.log.debug(
                "Table '%s': %d cols, PKs=%s, FKs=%d",
                tbl_name, len(tm.columns), tm.primary_keys, len(tm.foreign_keys),
            )

        return meta

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> psycopg.Connection:
        cfg = self.db_cfg
        dsn = (
            f"host={cfg['host']} port={cfg['port']} "
            f"dbname={cfg['dbname']} user={cfg['user']} password={cfg['password']}"
        )
        try:
            conn = psycopg.connect(dsn)
            self.log.info("Connected to PostgreSQL: %s/%s", cfg["host"], cfg["dbname"])
            return conn
        except Exception as exc:
            self.err_log.exception("Failed to connect to PostgreSQL: %s", exc)
            raise

    def _fetch_tables(self, conn: psycopg.Connection) -> list[str]:
        sql = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            return [row[0] for row in cur.fetchall()]

    def _fetch_columns(self, conn: psycopg.Connection) -> dict[str, list[ColumnMeta]]:
        sql = """
            SELECT
                table_name,
                column_name,
                data_type,
                udt_name,
                is_nullable,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
        """
        result: dict[str, list[ColumnMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for row in cur.fetchall():
                tbl, col, dtype, udt, nullable, max_len, num_prec, num_scale, pos = row
                col_meta = ColumnMeta(
                    name=col,
                    data_type=dtype,
                    udt_name=udt,
                    is_nullable=(nullable == "YES"),
                    character_maximum_length=max_len,
                    numeric_precision=num_prec,
                    numeric_scale=num_scale,
                    ordinal_position=pos,
                )
                result.setdefault(tbl, []).append(col_meta)
        return result

    def _fetch_primary_keys(self, conn: psycopg.Connection) -> dict[str, list[str]]:
        sql = """
            SELECT
                kcu.table_name,
                kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema    = %s
            ORDER BY kcu.table_name, kcu.ordinal_position
        """
        result: dict[str, list[str]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for tbl, col in cur.fetchall():
                result.setdefault(tbl, []).append(col)
        return result

    def _fetch_foreign_keys(self, conn: psycopg.Connection) -> dict[str, list[ForeignKeyMeta]]:
        sql = """
            SELECT
                tc.table_name            AS fk_table,
                tc.constraint_name,
                kcu.column_name          AS fk_column,
                ccu.table_name           AS ref_table,
                ccu.column_name          AS ref_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
               AND ccu.table_schema   = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema    = %s
            ORDER BY tc.table_name, tc.constraint_name
        """
        result: dict[str, list[ForeignKeyMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for fk_tbl, cname, fk_col, ref_tbl, ref_col in cur.fetchall():
                fk = ForeignKeyMeta(
                    constraint_name=cname,
                    column=fk_col,
                    ref_table=ref_tbl,
                    ref_column=ref_col,
                )
                result.setdefault(fk_tbl, []).append(fk)
        return result