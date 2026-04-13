"""
file_writer.py
--------------
Streams generated row batches to a CSV file on disk.

Design decisions
----------------
* Uses the csv module (stdlib) for correctness with quoting edge-cases.
* Writes NULL as an empty string — PostgreSQL COPY interprets that correctly
  by default (or via NULL '' in the COPY command).
* Supports streaming: each batch is appended to the same file handle,
  avoiding a full in-memory materialisation of all rows.
* Returns the output file path for handoff to postgres_loader.py.
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path
from typing import Generator, Any

logger = logging.getLogger("app")

# PostgreSQL COPY default NULL representation
_PG_NULL = ""


class FileWriter:
    """
    Writes row batches to a single CSV file per table.
    """

    def __init__(self, table_name: str, column_names: list[str], config: dict, loggers: dict):
        self.table_name = table_name
        self.column_names = column_names
        self.log = loggers["app"]
        self.err_log = loggers["error"]
        self.audit = loggers["audit"]

        output_dir = Path(config.get("generation", {}).get("output_dir", "./output"))
        output_dir.mkdir(parents=True, exist_ok=True)
        self.filepath = output_dir / f"{table_name}.csv"

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def write_all(
        self,
        batch_generator: Generator[list[dict], None, None],
        total_rows: int,
    ) -> Path:
        """
        Consume all batches from the generator, write to CSV.
        Returns the path to the completed file.
        """
        rows_written = 0

        with open(self.filepath, mode="w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(
                fh,
                delimiter=",",
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
            )

            # Write header row
            writer.writerow(self.column_names)

            for batch in batch_generator:
                for row in batch:
                    writer.writerow(self._serialize_row(row))
                rows_written += len(batch)

        file_size_mb = self.filepath.stat().st_size / (1024 * 1024)
        self.log.info(
            "CSV written: %s  (%d rows, %.2f MB)",
            self.filepath, rows_written, file_size_mb,
        )
        self.audit.info(
            "FILE | table=%-35s rows=%10d size_mb=%.2f path=%s",
            self.table_name, rows_written, file_size_mb, self.filepath,
        )
        return self.filepath

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------

    def _serialize_row(self, row: dict) -> list[str]:
        """Convert a row dict to an ordered list of CSV-safe strings."""
        result = []
        for col in self.column_names:
            val = row.get(col)
            if val is None:
                result.append(_PG_NULL)
            elif isinstance(val, bool):
                # CSV needs 't'/'f' for PostgreSQL boolean COPY
                result.append("t" if val else "f")
            elif isinstance(val, bytes):
                # Hex-escape bytes for PostgreSQL bytea
                result.append("\\x" + val.hex())
            else:
                result.append(str(val))
        return result