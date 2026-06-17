# /loci_platform/platform/airflow/dags/loci/collectors/cms/collector.py
"""
CMSCollector — orchestrates collection of data.cms.gov datasets.

Update scheme:
    The unit of work is a published dataset version (vintage). CMS
    publishes discrete annual/monthly versions rather than an
    append-only stream, and occasionally re-releases a version with
    corrections (signaled by the distribution-level modified date).

    collect(spec, force=False) collects only vintages that are missing,
    incomplete, or re-released since last ingest. force=True recollects
    every vintage. Either way, ingestion is StagedIngest in SCD2 mode
    keyed on spec.entity_key (which includes "vintage"), so recollection
    is always safe — unchanged rows dedupe away.

Freshness check (per vintage):
    A vintage is considered fully ingested when both hold:
      1. current rows in the target for that vintage >= the source's
         /stats row count (>=, not ==, because rows removed by a CMS
         correction stay current in the target — see Limitations)
      2. the stored _source_modified is >= the catalog's modified date
    After each successful ingest, _source_modified is advanced on all
    of the vintage's current rows. This is hash-excluded metadata, so
    it doesn't disturb SCD2 history; without it, a re-release whose
    rows mostly dedupe away would fail check 2 forever and re-download
    the vintage on every run.

Retrieval (driven by the spec):
    spec.retrieval == "api": page rows out of the versioned JSON API
    spec.retrieval == "csv": download the version's CSV distribution
        and parse it with the streaming CSV parser

Limitations (deliberate, until needed):
    - Rows removed by a CMS correction are not invalidated; they remain
      current in the target. StagedIngest's invalidate_missing can't be
      used here because staging only ever holds one vintage, so it
      would close out every other vintage in the table.
    - Schema drift is warn-and-filter: source columns missing from the
      target table are dropped with a warning, not an error.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

from loci.collectors.cms.client import CMSClient
from loci.collectors.cms.metadata import CMSDatasetVersion, CMSMetadata
from loci.collectors.cms.spec import CMSDatasetSpec
from loci.parsers.csv_parser import parse_csv
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class CMSCollector:
    """
    Orchestrates collecting and ingesting data.cms.gov datasets defined
    by a CMSDatasetSpec.

    Parameters
    ----------
    engine : PostgresEngine
    client : CMSClient, optional
        Created with defaults if not provided (no auth required).
    tracker : IngestionTracker, optional
        If provided (or by default), logs each vintage run.
    """

    SOURCE_NAME = "cms"
    # Written by the pipeline, never present in source rows; excluded
    # from the staging column list by StagedIngest.
    PIPELINE_COLUMNS = {"ingested_at", "record_hash", "valid_from", "valid_to"}
    # Written by this collector, but excluded from the record hash so a
    # re-release with identical content doesn't create new versions.
    SOURCE_METADATA_COLUMNS = {"_source_modified"}

    def __init__(
        self,
        engine: Any,
        client: CMSClient | None = None,
        tracker: IngestionTracker | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.engine = engine
        self.client = client or CMSClient()
        self.metadata = CMSMetadata(self.client)
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.logger = logger or logging.getLogger("cms_collector")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: CMSDatasetSpec, force: bool = False) -> dict[str, Any]:
        """
        Collect and ingest all vintages defined by a CMSDatasetSpec.

        Parameters
        ----------
        spec : CMSDatasetSpec
        force : bool
            If True, skip freshness checks and recollect every vintage.

        Returns a summary dict with counts.
        """
        versions = self._resolve_versions(spec)

        summary: dict[str, Any] = {
            "spec_name": spec.name,
            "versions_processed": 0,
            "versions_skipped": 0,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "errors": [],
        }

        for version in versions:
            try:
                if not force and self._already_ingested(spec, version):
                    self.logger.info(
                        "Skipping %s vintage=%s (already ingested, source unchanged)",
                        spec.name,
                        version.vintage,
                    )
                    summary["versions_skipped"] += 1
                    continue

                staged, merged = self._collect_version(spec, version)
                summary["versions_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
            except Exception as e:
                self.logger.error("Failed: %s vintage=%s: %s", spec.name, version.vintage, e)
                summary["errors"].append({"vintage": version.vintage, "error": str(e)})

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    def generate_ddl(self, spec: CMSDatasetSpec) -> str:
        """
        Generate a CREATE TABLE statement for a CMSDatasetSpec.

        Samples one row from every vintage in scope and unions the
        column sets (newest first), so columns that appear or disappear
        across vintages all get a home. All source columns are text;
        casting is a downstream concern.
        """
        columns = self._resolve_columns(spec)
        fqn = f"{spec.target_schema}.{spec.target_table}"

        lines = [f"create table {fqn} ("]
        for col in columns:
            lines.append(f'    "{col}" text,')
        lines.append('    "vintage" text not null,')
        lines.append('    "_source_modified" text,')
        lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
        lines.append('    "record_hash" text not null,')
        lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
        lines.append('    "valid_to" timestamptz')
        lines.append(");")

        ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint uq_{spec.target_table}_entity_hash")
        lines.append(f'    unique ({ek_cols}, "record_hash");')
        lines.append("")
        lines.append(f"create index ix_{spec.target_table}_current")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        return "\n".join(lines)

    def print_ddl(self, spec: CMSDatasetSpec) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        print(self.generate_ddl(spec))

    # ------------------------------------------------------------------
    # Version resolution / freshness
    # ------------------------------------------------------------------

    def _resolve_versions(self, spec: CMSDatasetSpec) -> list[CMSDatasetVersion]:
        """Resolve the spec's dataset title to its in-scope versions."""
        dataset = self.metadata.get_dataset(spec.dataset_title)
        versions = self.metadata.versions(dataset)
        if spec.vintages is not None:
            wanted = set(spec.vintages)
            found = {v.vintage for v in versions}
            missing = wanted - found
            if missing:
                self.logger.warning(
                    "Spec %r requests vintages not in the catalog: %s (available: %s)",
                    spec.name,
                    sorted(missing),
                    sorted(found),
                )
            versions = [v for v in versions if v.vintage in wanted]
        return versions

    def _already_ingested(self, spec: CMSDatasetSpec, version: CMSDatasetVersion) -> bool:
        """
        Return True if this vintage is fully ingested and the source
        hasn't been re-released since. See the module docstring for the
        two conditions.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select count(*) as n, max("_source_modified") as max_mod
                from {fqn}
                where "vintage" = %(vintage)s
                  and "valid_to" is null
                """,
                {"vintage": version.vintage},
            )
        except Exception:
            return False  # Table might not exist yet on first run

        n = int(df["n"].iloc[0])
        max_mod = df["max_mod"].iloc[0]
        if n == 0 or max_mod is None:
            return False

        # ISO date strings compare correctly as text.
        if version.modified and max_mod < version.modified:
            self.logger.info(
                "%s vintage=%s was re-released (stored %s < catalog %s)",
                spec.name,
                version.vintage,
                max_mod,
                version.modified,
            )
            return False

        if version.api_uuid:
            source_count = self.client.row_count(version.api_uuid)
            if n < source_count:
                self.logger.info(
                    "%s vintage=%s is incomplete (%d of %d rows)",
                    spec.name,
                    version.vintage,
                    n,
                    source_count,
                )
                return False

        return True

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def _collect_version(self, spec: CMSDatasetSpec, version: CMSDatasetVersion) -> tuple[int, int]:
        """
        Fetch and ingest one vintage. Returns (rows_staged, rows_merged).
        """
        self.logger.info(
            "Collecting %s vintage=%s via %s", spec.name, version.vintage, spec.retrieval
        )

        fqn = f"{spec.target_schema}.{spec.target_table}"
        table_columns = self._get_table_columns(spec.target_table, spec.target_schema)
        if not table_columns:
            raise RuntimeError(
                f"Target table {fqn} has no columns (does it exist?). "
                f"Run collector.print_ddl(spec) and create it first."
            )

        run_metadata = {
            "vintage": version.vintage,
            "source_modified": version.modified,
            "retrieval": spec.retrieval,
            "api_uuid": version.api_uuid,
            "csv_url": version.csv_url,
        }
        dataset_id = f"{spec.dataset_title}/{version.vintage}"

        with self.tracker.track(self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata) as run:
            with self.engine.staged_ingest(
                target_table=spec.target_table,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key,
                metadata_columns=self.PIPELINE_COLUMNS,
                hash_exclude_columns=self.SOURCE_METADATA_COLUMNS,
            ) as stager:
                first_batch = True
                for batch in self._iter_batches(spec, version):
                    batch = self._prepare_batch(batch, version)
                    if first_batch:
                        self._warn_on_column_drift(batch, table_columns, fqn)
                        first_batch = False
                    batch = self._filter_to_table_columns(batch, table_columns)
                    stager.write_batch(batch)

            run.rows_staged = stager.rows_staged
            run.rows_merged = stager.rows_merged
            run.rows_ingested = stager.rows_merged
            run.high_water_mark = version.modified

            if stager.rows_staged == 0:
                self.logger.warning(
                    "No rows returned for %s vintage=%s", spec.name, version.vintage
                )
            else:
                self._advance_source_modified(spec, version)

        self.logger.info(
            "Ingested %s vintage=%s: staged=%d merged=%d",
            spec.name,
            version.vintage,
            stager.rows_staged,
            stager.rows_merged,
        )
        return stager.rows_staged, stager.rows_merged

    def _iter_batches(self, spec: CMSDatasetSpec, version: CMSDatasetVersion):
        """Yield batches of raw source rows via the spec's retrieval mode."""
        if spec.retrieval == "api":
            if not version.api_uuid:
                raise ValueError(
                    f"Vintage {version.vintage} has no API distribution; "
                    f"set retrieval='csv' on the spec."
                )
            yield from self.client.iter_pages(version.api_uuid)
        else:
            if not version.csv_url:
                raise ValueError(
                    f"Vintage {version.vintage} has no CSV distribution; "
                    f"set retrieval='api' on the spec."
                )
            tmp = tempfile.NamedTemporaryFile(suffix=".csv", prefix="cms_", delete=False)
            tmp.close()
            filepath = Path(tmp.name)
            try:
                self.logger.info("Downloading %s", version.csv_url)
                self.client.download_csv(version.csv_url, filepath)
                self.logger.info(
                    "Downloaded to %s (%.1f MB)",
                    filepath,
                    filepath.stat().st_size / (1024 * 1024),
                )
                yield from parse_csv(filepath)
            finally:
                filepath.unlink(missing_ok=True)

    def _prepare_batch(
        self, batch: list[dict[str, Any]], version: CMSDatasetVersion
    ) -> list[dict[str, Any]]:
        """Normalize column names and stamp the vintage metadata columns."""
        prepared = []
        for row in batch:
            row = {self._normalize_column(k): v for k, v in row.items()}
            row["vintage"] = version.vintage
            row["_source_modified"] = version.modified
            prepared.append(row)
        return prepared

    @staticmethod
    def _normalize_column(name: str) -> str:
        """Lowercase column names so they're queryable without quotes."""
        return name.strip().lstrip("\ufeff").lower().replace(" ", "_")

    def _advance_source_modified(self, spec: CMSDatasetSpec, version: CMSDatasetVersion) -> None:
        """
        Advance _source_modified on the vintage's current rows.

        Rows whose content didn't change in a re-release are deduped by
        StagedIngest and keep their old stamp; without this update the
        freshness check would re-download the vintage on every run.
        """
        if not version.modified:
            return
        fqn = f"{spec.target_schema}.{spec.target_table}"
        self.engine.execute(
            f"""
            update {fqn}
            set "_source_modified" = %(modified)s
            where "vintage" = %(vintage)s
              and "valid_to" is null
              and ("_source_modified" is null or "_source_modified" < %(modified)s)
            """,
            {"modified": version.modified, "vintage": version.vintage},
        )

    # ------------------------------------------------------------------
    # Table / column helpers
    # ------------------------------------------------------------------

    def _resolve_columns(self, spec: CMSDatasetSpec) -> list[str]:
        """
        Union the normalized column sets across all in-scope vintages,
        preserving newest-version column order, for DDL generation.
        """
        versions = [v for v in self._resolve_versions(spec) if v.api_uuid]
        if not versions:
            raise ValueError(f"No API-accessible versions found for {spec.dataset_title!r}")

        columns: list[str] = []
        seen: set[str] = set()
        for version in reversed(versions):  # newest first
            for col in self.metadata.columns(version.api_uuid):
                col = self._normalize_column(col)
                if col not in seen:
                    seen.add(col)
                    columns.append(col)
        return columns

    def _get_table_columns(self, target_table: str, target_schema: str) -> set[str]:
        df = self.engine.query(
            """
            select column_name
            from information_schema.columns
            where table_schema = %(schema)s and table_name = %(table)s
            """,
            {"schema": target_schema, "table": target_table},
        )
        return set(df["column_name"])

    def _warn_on_column_drift(
        self, batch: list[dict[str, Any]], table_columns: set[str], fqn: str
    ) -> None:
        """Warn-only comparison of incoming columns vs the target table."""
        source_columns = set()
        for row in batch:
            source_columns.update(row.keys())

        data_columns_in_table = table_columns - self.PIPELINE_COLUMNS
        new_in_source = source_columns - data_columns_in_table
        missing_from_source = data_columns_in_table - source_columns - self.SOURCE_METADATA_COLUMNS

        if new_in_source:
            self.logger.warning(
                "Source has columns not in %s (they will be dropped): %s. "
                "Add them via migration to keep them.",
                fqn,
                sorted(new_in_source),
            )
        if missing_from_source:
            self.logger.warning(
                "Columns in %s but not in this vintage's source: %s",
                fqn,
                sorted(missing_from_source),
            )

    def _filter_to_table_columns(
        self, rows: list[dict[str, Any]], table_columns: set[str]
    ) -> list[dict[str, Any]]:
        for row in rows:
            for k in set(row.keys()) - table_columns:
                del row[k]
        return rows
