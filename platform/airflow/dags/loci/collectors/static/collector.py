"""
StaticFileCollector — orchestrates collection of StaticFileDatasetSpec datasets.

Follows the house collector pattern (cf. CMSCollector, OSMCollector):
takes an engine, an optional client and tracker; collect() writes rows
via the engine's StagedIngest (SCD2 keyed by spec.entity_key) and
returns a summary dict; generate_ddl() emits the CREATE TABLE for a
spec's target.

Update semantics for static files:
    collect(spec, force=False) — load only files whose vintage is not yet
        present in the target table. New-edition detection *is* the
        incremental story for annually published files.
    collect(spec, force=True)  — re-download and re-ingest every file in
        the manifest. Use when a publisher revises a file in place (AHRQ
        does this occasionally); SCD2 turns unchanged rows into no-ops.

Usage:
    spec = StaticFileDatasetSpec(...)
    collector = StaticFileCollector(engine=engine)

    # Generate DDL for a new table:
    collector.print_ddl(spec)

    # Collect (skips vintages already in the target):
    summary = collector.collect(spec, force=False)
"""

from __future__ import annotations

import logging
from typing import Any

from loci.collectors.static.client import StaticFileClient
from loci.collectors.static.spec import FileRef, StaticFileDatasetSpec
from loci.db.core import PostgresEngine
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class StaticFileCollector:
    SOURCE_NAME = "static_file"
    # Filled by DDL defaults / StagedIngest's SCD2 merge, never present
    # in source rows; excluded from the staging column list.
    PIPELINE_COLUMNS = {"ingested_at", "record_hash", "valid_from", "valid_to"}

    def __init__(
        self,
        engine: PostgresEngine,
        client: StaticFileClient | None = None,
        tracker: IngestionTracker | None = None,
    ):
        self.client = client or StaticFileClient()
        self.engine = engine
        self.tracker = tracker

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def collect(self, spec: StaticFileDatasetSpec, force: bool = False) -> dict[str, Any]:
        """Collect every file in the spec's manifest. Returns a summary dict."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        if not self._table_exists(spec):
            raise RuntimeError(
                f"Target table {fqn} does not exist. "
                f"Run collector.print_ddl(spec) and create it first."
            )

        summary: dict[str, Any] = {
            "dataset": spec.name,
            "files_processed": 0,
            "files_skipped": 0,
            "rows_staged": 0,
            "rows_merged": 0,
        }

        for file_ref in spec.files:
            if not force and self._already_ingested(spec, file_ref.vintage):
                logger.info(
                    "Skipping %s vintage=%s (already ingested; use force=True to refresh)",
                    spec.name,
                    file_ref.vintage,
                )
                summary["files_skipped"] += 1
                continue

            logger.info(
                "Collecting %s vintage=%s from %s", spec.name, file_ref.vintage, file_ref.url
            )
            rows = []
            for row in self.client.iter_rows(file_ref):
                row["vintage"] = file_ref.vintage
                rows.append(row)

            if not rows:
                logger.warning("No rows parsed from %s; skipping ingest", file_ref.url)
                continue

            staged, merged = self._tracked_ingest(spec, file_ref, rows)
            summary["files_processed"] += 1
            summary["rows_staged"] += staged
            summary["rows_merged"] += merged
            logger.info(
                "Ingested %s vintage=%s: staged=%d merged=%d",
                spec.name,
                file_ref.vintage,
                staged,
                merged,
            )

        return summary

    def _tracked_ingest(
        self, spec: StaticFileDatasetSpec, file_ref: FileRef, rows: list[dict[str, Any]]
    ) -> tuple[int, int]:
        if not self.tracker:
            return self._ingest_rows(spec, rows)

        with self.tracker.track(
            source=self.SOURCE_NAME,
            dataset_id=f"{spec.name}/{file_ref.vintage}",
            target_table=f"{spec.target_schema}.{spec.target_table}",
            metadata={
                "url": file_ref.url,
                "vintage": file_ref.vintage,
                "file_format": file_ref.file_format,
            },
        ) as run:
            staged, merged = self._ingest_rows(spec, rows)
            run.rows_staged = staged
            run.rows_merged = merged
        return staged, merged

    def _ingest_rows(
        self, spec: StaticFileDatasetSpec, rows: list[dict[str, Any]]
    ) -> tuple[int, int]:
        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=self.PIPELINE_COLUMNS,
        ) as stager:
            stager.write_batch(rows)
        return stager.rows_staged, stager.rows_merged

    # ------------------------------------------------------------------
    # Freshness / table helpers
    # ------------------------------------------------------------------

    def _already_ingested(self, spec: StaticFileDatasetSpec, vintage: str) -> bool:
        """Return True if any rows for this vintage exist in the target.

        Caller has already verified the table exists, so query errors
        are real errors and are allowed to raise.
        """
        df = self.engine.query(
            f'select 1 from "{spec.target_schema}"."{spec.target_table}" '
            "where vintage = %(vintage)s limit 1",
            {"vintage": vintage},
        )
        return not df.empty

    def _table_exists(self, spec: StaticFileDatasetSpec) -> bool:
        df = self.engine.query(
            """
            select 1 from information_schema.tables
            where table_schema = %(schema)s and table_name = %(table)s
            limit 1
            """,
            {"schema": spec.target_schema, "table": spec.target_table},
        )
        return not df.empty

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def generate_ddl(self, spec: StaticFileDatasetSpec) -> str:
        """
        Generate CREATE TABLE DDL for the spec's target table by reading
        the header of the first file in the manifest.

        All source columns are text (see StaticFileDatasetSpec docstring
        for why), plus the vintage column and ingested_at. When the spec
        has an entity_key, the SCD2 columns (record_hash, valid_from,
        valid_to), the entity/hash uniqueness constraint, and the
        partial current-rows index are added; entity_key=None means
        append-only, so none of the SCD2 apparatus applies.
        """
        first_row = next(self.client.iter_rows(spec.files[0]), None)
        if first_row is None:
            raise ValueError(f"Could not read any rows from {spec.files[0].url} to derive columns")

        fqn = f"{spec.target_schema}.{spec.target_table}"

        col_defs = [f'    "{col}" text' for col in first_row.keys()]
        col_defs.append('    "vintage" text not null')
        col_defs.append(
            "    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC')"
        )
        if spec.entity_key:
            col_defs.append('    "record_hash" text not null')
            col_defs.append(
                "    \"valid_from\" timestamptz not null default (now() at time zone 'UTC')"
            )
            col_defs.append('    "valid_to" timestamptz')

        ddl = f"create table if not exists {fqn} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);\n"

        if spec.entity_key:
            ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
            ddl += (
                f"\nalter table {fqn}\n"
                f"    add constraint uq_{spec.target_table}_entity_hash\n"
                f'    unique ({ek_cols}, "record_hash");\n'
            )
            ddl += (
                f"\ncreate index if not exists ix_{spec.target_table}_current\n"
                f"    on {fqn} ({ek_cols})\n"
                f"    where valid_to is null;\n"
            )

        return ddl

    def print_ddl(self, spec: StaticFileDatasetSpec) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        print(self.generate_ddl(spec))
