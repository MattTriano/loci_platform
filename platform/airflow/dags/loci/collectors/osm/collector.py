# /loci_platform/platform/airflow/dags/loci/collectors/osm/collector.py
"""
OSM Overpass API collection orchestrator.

Wires together the OSMDatasetSpec, OSMClient, and the engine's staged_ingest
to perform full and incremental collection of OSM data.

Usage:
    spec = OSMDatasetSpec(...)
    collector = OSMCollector(engine=engine)

    # Generate DDL for a new table:
    print(collector.generate_ddl(spec))

    # Full refresh (catches deletions via invalidate_missing):
    summary = collector.collect(spec, force=True)

    # Incremental update (uses (newer:) filter):
    summary = collector.collect(spec, force=False)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from loci.collectors.osm.client import OSMClient
from loci.collectors.osm.spec import OSMDatasetSpec
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


# Metadata columns: filled in by the database side of staged_ingest
# (defaults, SCD2 merge logic, etc.) rather than by us per-row. These
# are excluded from the COPY column list. `ingested_at` is NOT in this
# set — we set it explicitly on every row in this collector.
METADATA_COLUMNS: set[str] = {
    "record_hash",
    "valid_from",
    "valid_to",
}

# Hash exclusions: metadata columns plus `ingested_at` (we set it per
# run, but it shouldn't make every run a new SCD2 version) plus OSM
# bookkeeping that changes every edit but doesn't represent a
# meaningful version difference for our purposes.
HASH_EXCLUDE_COLUMNS: set[str] = METADATA_COLUMNS | {
    "ingested_at",
    "osm_version",
    "osm_timestamp",
}


# Default flush threshold for batched ingestion.
DEFAULT_BATCH_SIZE = 50000


class OSMCollector:
    """
    Orchestrate OSM data collection via the Overpass API.

    Parameters
    ----------
    engine : PostgresEngine
    client : OSMClient, optional
        Defaults to a new OSMClient against the public Overpass endpoint.
    tracker : IngestionTracker, optional
    batch_size : int
        Number of rows to accumulate before flushing via
        staged_ingest.write_batch().
    """

    SOURCE_NAME = "osm"

    def __init__(
        self,
        engine,
        client: OSMClient | None = None,
        tracker: IngestionTracker | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.engine = engine
        self.client = client or OSMClient()
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.batch_size = batch_size
        self.logger = logging.getLogger("osm_collector")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: OSMDatasetSpec, force: bool = False) -> dict[str, Any]:
        """
        Collect data for a spec and merge into the target table.

        force=True  -> full pull, invalidate_missing=True (catches deletions)
        force=False -> incremental via (newer:<high_water_mark>),
                       invalidate_missing=False
                       Falls back to full pull if the target table is empty.

        Raises if the target table doesn't exist.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        if not self._table_exists(spec):
            raise RuntimeError(
                f"Target table {fqn} does not exist. Run generate_ddl() and create the table first."
            )

        ingested_at = datetime.now(UTC)
        date_filter = self._resolve_date_filter(spec, force)
        mode = "full" if date_filter is None else "incremental"

        self.logger.info(
            "Collecting %s: mode=%s, date_filter=%s, ingested_at=%s",
            spec.name,
            mode,
            date_filter,
            ingested_at.isoformat(),
        )

        run_metadata = {
            "mode": mode,
            "date_filter": date_filter,
            "ingested_at": ingested_at.isoformat(),
        }

        with self.tracker.track(
            source=self.SOURCE_NAME,
            dataset_id=spec.name,
            target_table=fqn,
            metadata=run_metadata,
        ) as run:
            staged, merged, invalidated, elements_fetched = self._run_ingestion(
                spec=spec,
                date_filter=date_filter,
                ingested_at=ingested_at,
                invalidate_missing=force,
            )

            run.rows_staged = staged
            run.rows_merged = merged
            run.metadata["elements_fetched"] = elements_fetched
            run.metadata["rows_invalidated"] = invalidated

        summary = {
            "spec_name": spec.name,
            "mode": mode,
            "date_filter": date_filter,
            "ingested_at": ingested_at.isoformat(),
            "elements_fetched": elements_fetched,
            "rows_staged": staged,
            "rows_merged": merged,
            "rows_invalidated": invalidated,
        }
        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    def generate_ddl(self, spec: OSMDatasetSpec) -> str:
        """
        Build the CREATE TABLE statement for an OSMDatasetSpec.

        Generates the fixed OSM columns (osm_type, osm_id, osm_version,
        osm_timestamp, geom, tags), one text column per promoted tag,
        the SCD2 metadata columns, the entity-key uniqueness constraint,
        a partial index on (osm_type, osm_id) for current rows, and a
        partial GIST index on geom for current rows.
        """
        return _build_ddl(spec)

    def print_ddl(self, spec: OSMDatasetSpec) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        print(self.generate_ddl(spec))

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def _run_ingestion(
        self,
        spec: OSMDatasetSpec,
        date_filter: str | None,
        ingested_at: datetime,
        invalidate_missing: bool,
    ) -> tuple[int, int, int, int]:
        """
        Stream rows from the client, batch them, write to staged_ingest.

        Returns (rows_staged, rows_merged, rows_invalidated, elements_fetched).
        """
        elements_fetched = 0
        batch: list[dict[str, Any]] = []

        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=METADATA_COLUMNS,
            hash_exclude_columns=HASH_EXCLUDE_COLUMNS,
            invalidate_missing=invalidate_missing,
        ) as stager:
            for row in self.client.fetch_rows(spec, date_filter=date_filter):
                row["ingested_at"] = ingested_at
                elements_fetched += 1
                batch.append(row)

                if len(batch) >= self.batch_size:
                    self.logger.info("Flushing batch: %d rows", len(batch))
                    stager.write_batch(batch)
                    batch = []

            if batch:
                self.logger.info("Flushing final batch: %d rows", len(batch))
                stager.write_batch(batch)

        return (
            stager.rows_staged,
            stager.rows_merged,
            stager.rows_invalidated,
            elements_fetched,
        )

    # ------------------------------------------------------------------
    # Incremental floor lookup
    # ------------------------------------------------------------------

    def _resolve_date_filter(self, spec: OSMDatasetSpec, force: bool) -> str | None:
        """
        Decide whether this run is full or incremental.

        Returns:
            None             -> full pull (force=True or table is empty)
            ISO timestamp    -> incremental pull, used as (newer:) floor
        """
        if force:
            return None

        high_water = self._high_water_mark(spec)
        if high_water is None:
            self.logger.info(
                "No prior ingestion data for %s; falling back to full pull.",
                spec.name,
            )
            return None

        return _format_overpass_timestamp(high_water)

    def _high_water_mark(self, spec: OSMDatasetSpec) -> datetime | None:
        """
        Return max(ingested_at) from the target table, or None if the
        table is empty.

        Caller has already verified the table exists.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        df = self.engine.query(f"select max(ingested_at) as mx from {fqn}")
        if df.empty:
            return None
        value = df.iloc[0]["mx"]
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value

    def _table_exists(self, spec: OSMDatasetSpec) -> bool:
        df = self.engine.query(
            """
            select 1 from information_schema.tables
            where table_schema = %(schema)s and table_name = %(table)s
            limit 1
            """,
            {"schema": spec.target_schema, "table": spec.target_table},
        )
        return not df.empty


# ----------------------------------------------------------------------
# Helpers (module-level so they're easy to test in isolation)
# ----------------------------------------------------------------------


def _format_overpass_timestamp(dt: datetime) -> str:
    """
    Format a datetime as an Overpass-friendly UTC ISO timestamp.

    Overpass accepts ISO-8601 timestamps with a trailing Z. We force
    UTC and microseconds-stripped output for cleanliness.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------------
# DDL builder
# ----------------------------------------------------------------------


def _build_ddl(spec: OSMDatasetSpec) -> str:
    """Render the CREATE TABLE plus index/constraint DDL for an OSMDatasetSpec."""
    fqn = f"{spec.target_schema}.{spec.target_table}"

    col_defs = [
        '    "osm_type" text not null',
        '    "osm_id" bigint not null',
        '    "osm_version" integer',
        '    "osm_timestamp" timestamptz',
        '    "geom" geometry(Geometry, 4326)',
        '    "tags" jsonb',
        '    "node_ids" bigint[]',
    ]

    for column_name in spec.promoted_columns:
        col_defs.append(f'    "{column_name}" text')

    col_defs.append('    "ingested_at" timestamptz not null')
    col_defs.append('    "record_hash" text not null')
    col_defs.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC')")
    col_defs.append('    "valid_to" timestamptz')

    ddl = f"create table if not exists {fqn} (\n"
    ddl += ",\n".join(col_defs)
    ddl += "\n);\n"

    ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
    constraint_name = f"uq_{spec.target_table}_entity_hash"
    current_index_name = f"ix_{spec.target_table}_current"
    geom_index_name = f"ix_{spec.target_table}_geom"

    ddl += (
        f"\nalter table {fqn}\n"
        f"    add constraint {constraint_name}\n"
        f'    unique ({ek_cols}, "record_hash");\n'
    )
    ddl += (
        f"\ncreate index if not exists {current_index_name}\n"
        f"    on {fqn} ({ek_cols})\n"
        f"    where valid_to is null;\n"
    )
    ddl += (
        f"\ncreate index if not exists {geom_index_name}\n"
        f"    on {fqn} using gist (geom)\n"
        f"    where valid_to is null;\n"
    )

    return ddl
