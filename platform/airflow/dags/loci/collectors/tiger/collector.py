"""
TIGER/Line geospatial data collection and ingestion pipeline.

Classes:
    TigerDatasetSpec  — defines what TIGER data to collect
    TigerCollector    — orchestrates download, parsing, and ingestion to PostGIS

Usage:
    from tiger_collector import TigerDatasetSpec, TigerCollector

    spec = TigerDatasetSpec(
        name="census_tracts",
        layer="TRACT",
        vintages=[2023, 2024],
        target_table="census_tracts",
        target_schema="raw_data",
        state_fips=["17"],
    )

    collector = TigerCollector(engine=engine)
    collector.collect(spec)
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import requests
from loci.collectors.tiger.metadata import TigerMetadata
from loci.collectors.tiger.spec import _CANDIDATE_ID_COLUMNS, TigerDatasetSpec
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


METADATA_COLUMNS: set[str] = {
    "ingested_at",
    "record_hash",
    "valid_from",
    "valid_to",
}

# Metadata columns used when there is no entity key (no SCD2).
METADATA_COLUMNS_NO_SCD2: set[str] = {
    "ingested_at",
}


# ------------------------------------------------------------------ #
#  Collector
# ------------------------------------------------------------------ #


class TigerCollector:
    """
    Orchestrates downloading, parsing, and ingesting TIGER/Line or
    Cartographic Boundary shapefiles.

    Iterates over vintages × states (or handles national files),
    checks the target table for already-ingested data, downloads
    the shapefile zip to a temp file, parses features via the
    shapefile parser, and ingests via StagedIngest with SCD2 merge
    (when an entity key is available) or plain insert (when not).

    Parameters
    ----------
    engine : PostgresEngine
    tracker : IngestionTracker, optional
    """

    def __init__(self, engine, tracker=None):
        self.engine = engine
        self.tracker = tracker
        self._metadata = TigerMetadata()
        self._session = requests.Session()
        self.logger = logging.getLogger("tiger_collector")

    def collect(self, spec: TigerDatasetSpec, force: bool = False) -> dict:
        """
        Collect and ingest all data defined by a TigerDatasetSpec.

        Parameters
        ----------
        spec : TigerDatasetSpec
        force : bool
            If True, skip idempotency checks and re-ingest everything.
            For datasets without an entity key, this may result in
            duplicate rows.

        Returns a summary dict with counts.
        """
        summary = {
            "spec_name": spec.name,
            "vintages_processed": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "errors": [],
        }

        for vintage in spec.vintages:
            if spec.scope == "national":
                self._collect_national(spec, vintage, force, summary)
            elif spec.scope == "county":
                self._collect_county_based(spec, vintage, force, summary)
            else:
                self._collect_state_based(spec, vintage, force, summary)

            summary["vintages_processed"] += 1

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    # ------------------------------------------------------------------ #
    #  Scope-specific iteration
    # ------------------------------------------------------------------ #

    def _collect_national(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        force: bool,
        summary: dict,
    ) -> None:
        """Handle a national-scope layer (single file per vintage)."""
        if not force and self._already_ingested(spec, vintage, state_fips=None):
            self.logger.info(
                "Skipping %s vintage=%d (national, already ingested)",
                spec.name,
                vintage,
            )
            summary["files_skipped"] += 1
            return

        url = self._metadata.get_download_url(
            vintage=vintage,
            layer=spec.layer,
            source=spec.source,
            state_fips=None,
            resolution=spec.resolution,
        )

        try:
            staged, merged = self._download_parse_ingest(
                spec,
                vintage,
                url,
                state_fips=None,
            )
            summary["files_processed"] += 1
            summary["total_rows_staged"] += staged
            summary["total_rows_merged"] += merged
        except Exception as e:
            self.logger.error(
                "Failed: %s vintage=%d (national): %s",
                spec.name,
                vintage,
                e,
            )
            summary["errors"].append(
                {
                    "vintage": vintage,
                    "state_fips": None,
                    "error": str(e),
                }
            )

    def _collect_state_based(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        force: bool,
        summary: dict,
    ) -> None:
        """Handle a state-based layer (one file per state per vintage)."""
        for state_fips in spec.states:
            if not force and self._already_ingested(spec, vintage, state_fips):
                self.logger.info(
                    "Skipping %s vintage=%d state=%s (already ingested)",
                    spec.name,
                    vintage,
                    state_fips,
                )
                summary["files_skipped"] += 1
                continue

            url = self._metadata.get_download_url(
                vintage=vintage,
                layer=spec.layer,
                source=spec.source,
                state_fips=state_fips,
                resolution=spec.resolution,
            )

            try:
                staged, merged = self._download_parse_ingest(
                    spec,
                    vintage,
                    url,
                    state_fips=state_fips,
                )
                summary["files_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
            except Exception as e:
                self.logger.error(
                    "Failed: %s vintage=%d state=%s: %s",
                    spec.name,
                    vintage,
                    state_fips,
                    e,
                )
                summary["errors"].append(
                    {
                        "vintage": vintage,
                        "state_fips": state_fips,
                        "error": str(e),
                    }
                )

    def _collect_county_based(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        force: bool,
        summary: dict,
    ) -> None:
        """
        Handle a county-based layer (one file per county per vintage).

        Lists files from the server to discover county FIPS codes, then
        filters to the requested states.
        """
        files_df = self._metadata.list_files(vintage, spec.layer, source=spec.source)
        if files_df.empty:
            self.logger.warning(
                "No files found for %s vintage=%d",
                spec.name,
                vintage,
            )
            return

        for _, row in files_df.iterrows():
            filename = row["filename"]
            url = row["url"]
            state_fips = row.get("state_fips")

            # For county-based files, state_fips from the filename is the
            # first 2 digits of the 5-digit county FIPS. Filter to requested
            # states. County-based filenames use the full 5-digit FIPS as the
            # fips portion: tl_2024_01001_roads.zip
            county_fips = self._extract_county_fips(filename, vintage)
            file_state = county_fips[:2] if county_fips else state_fips

            if file_state and file_state not in spec.states:
                continue

            # Use county_fips as the unit for idempotency
            check_fips = county_fips or state_fips
            if not force and self._already_ingested_county(
                spec,
                vintage,
                check_fips,
            ):
                self.logger.info(
                    "Skipping %s vintage=%d county=%s (already ingested)",
                    spec.name,
                    vintage,
                    check_fips,
                )
                summary["files_skipped"] += 1
                continue

            try:
                staged, merged = self._download_parse_ingest(
                    spec,
                    vintage,
                    url,
                    state_fips=check_fips,
                )
                summary["files_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
            except Exception as e:
                self.logger.error(
                    "Failed: %s vintage=%d county=%s: %s",
                    spec.name,
                    vintage,
                    check_fips,
                    e,
                )
                summary["errors"].append(
                    {
                        "vintage": vintage,
                        "state_fips": check_fips,
                        "error": str(e),
                    }
                )

    # ------------------------------------------------------------------ #
    #  Core: download → parse → ingest
    # ------------------------------------------------------------------ #

    def _download_parse_ingest(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        url: str,
        state_fips: str | None,
    ) -> tuple[int, int]:
        """
        Download a shapefile zip, parse it, and ingest into the target table.

        Returns (rows_staged, rows_merged).
        """
        from loci.parsers.shapefile import parse_shapefile

        self.logger.info(
            "Collecting %s vintage=%d state=%s url=%s",
            spec.name,
            vintage,
            state_fips,
            url,
        )

        filepath = self._download_to_tempfile(url)

        try:
            geometry_column = self._get_geometry_column(spec)

            batches, parse_result = parse_shapefile(
                filepath=filepath,
                geometry_column=geometry_column,
                batch_size=5000,
                lowercase_columns=spec.lowercase_columns,
            )

            # Peek at the first batch to discover column names, then
            # chain it back so _ingest_batches sees all rows.
            from itertools import chain

            first_batch = next(batches, [])
            row_keys = list(first_batch[0].keys()) if first_batch else []
            batches = chain([first_batch], batches) if first_batch else batches

            entity_key = self._resolve_entity_key(spec, row_keys)
            use_scd2 = entity_key is not None

            # For county-scoped layers, if the data doesn't already have
            # statefp/countyfp columns, we inject them from the filename-
            # derived county FIPS so the table has useful filtering columns.
            synthetic_fips = {}
            if spec.scope == "county" and state_fips and len(state_fips) == 5:
                statefp_col = "statefp" if spec.lowercase_columns else "STATEFP"
                countyfp_col = "countyfp" if spec.lowercase_columns else "COUNTYFP"
                row_keys_lower = {k.lower() for k in row_keys}
                if "statefp" not in row_keys_lower:
                    synthetic_fips[statefp_col] = state_fips[:2]
                if "countyfp" not in row_keys_lower:
                    synthetic_fips[countyfp_col] = state_fips[2:]

            dataset_id = f"{spec.name}/{vintage}/{state_fips or 'national'}"

            if use_scd2:
                self.logger.info(
                    "Using SCD2 merge for %s (entity_key=%s)",
                    spec.name,
                    entity_key,
                )
            else:
                self.logger.info(
                    "No entity key for %s; using plain staged insert",
                    spec.name,
                )

            if self.tracker:
                with self.tracker.track(
                    source="tiger",
                    dataset_id=dataset_id,
                    target_table=f"{spec.target_schema}.{spec.target_table}",
                    metadata={
                        "vintage": vintage,
                        "state_fips": state_fips,
                        "layer": spec.layer,
                        "source": spec.source,
                        "url": url,
                        "use_scd2": use_scd2,
                    },
                ) as run:
                    staged, merged = self._ingest_batches(
                        spec,
                        vintage,
                        batches,
                        entity_key,
                        use_scd2,
                        synthetic_fips,
                    )
                    run.rows_staged = staged
                    run.rows_merged = merged
                    run.metadata["features_parsed"] = parse_result.features_parsed
                    run.metadata["features_failed"] = parse_result.features_failed
                    if parse_result.source_crs:
                        run.metadata["source_crs"] = parse_result.source_crs
            else:
                staged, merged = self._ingest_batches(
                    spec,
                    vintage,
                    batches,
                    entity_key,
                    use_scd2,
                    synthetic_fips,
                )

            self.logger.info(
                "Ingested %s vintage=%d state=%s: parsed=%d failed=%d staged=%d merged=%d",
                spec.name,
                vintage,
                state_fips,
                parse_result.features_parsed,
                parse_result.features_failed,
                staged,
                merged,
            )
            return staged, merged

        finally:
            filepath.unlink(missing_ok=True)
            self.logger.info("Cleaned up temp file %s", filepath)

    def _ingest_batches(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        batches,
        entity_key: list[str] | None,
        use_scd2: bool,
        synthetic_fips: dict[str, str] | None = None,
    ) -> tuple[int, int]:
        """
        Feed parsed batches into StagedIngest.

        When use_scd2 is True, uses SCD2 merge with entity_key.
        When False, uses plain staged insert (no merge, no record hashing).

        Adds a `vintage` column to each row. For county-scoped layers
        where the shapefile lacks statefp/countyfp, also injects those
        from the filename-derived FIPS code via synthetic_fips.

        Returns (rows_staged, rows_merged).
        """
        metadata_cols = METADATA_COLUMNS if use_scd2 else METADATA_COLUMNS_NO_SCD2
        synthetic_fips = synthetic_fips or {}

        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=entity_key,
            metadata_columns=metadata_cols,
        ) as stager:
            for batch in batches:
                for row in batch:
                    row["vintage"] = vintage
                    for col, val in synthetic_fips.items():
                        row[col] = val
                stager.write_batch(batch)

        return stager.rows_staged, stager.rows_merged

    # ------------------------------------------------------------------ #
    #  Download
    # ------------------------------------------------------------------ #

    @retry(
        retry=retry_if_exception_type(
            (ChunkedEncodingError, ConnectionError, ReadTimeout),
        ),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=10, max=120),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _download_to_tempfile(self, url: str) -> Path:
        """Download a URL to a temp file. Returns the file path."""
        self.logger.info("Downloading %s", url)
        resp = self._session.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(
            suffix=".zip",
            prefix="tiger_",
            delete=False,
        )
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.close()
            filepath = Path(tmp.name)
            size_mb = filepath.stat().st_size / (1024 * 1024)
            self.logger.info(
                "Downloaded %s to %s (%.1f MB)",
                url,
                filepath,
                size_mb,
            )
            return filepath
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------ #
    #  Idempotency
    # ------------------------------------------------------------------ #

    def _already_ingested(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        state_fips: str | None,
    ) -> bool:
        """
        Check if current data exists for this vintage + state
        (or vintage alone for national layers).

        For SCD2 tables, checks valid_to is null. For non-SCD2 tables,
        just checks if any rows exist for the vintage.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        statefp_col = "statefp" if spec.lowercase_columns else "STATEFP"

        # Check whether valid_to column exists to determine query style.
        # If the table doesn't exist yet, we haven't ingested anything.
        has_valid_to = self._table_has_column(fqn, "valid_to")
        valid_to_clause = 'and "valid_to" is null' if has_valid_to else ""

        try:
            if state_fips is None:
                df = self.engine.query(
                    f"""
                    select 1 from {fqn}
                    where vintage = %(vintage)s
                      {valid_to_clause}
                    limit 1
                    """,
                    {"vintage": vintage},
                )
            else:
                df = self.engine.query(
                    f"""
                    select 1 from {fqn}
                    where vintage = %(vintage)s
                      and "{statefp_col}" = %(state)s
                      {valid_to_clause}
                    limit 1
                    """,
                    {"vintage": vintage, "state": state_fips},
                )
            return not df.empty
        except Exception:
            return False

    def _already_ingested_county(
        self,
        spec: TigerDatasetSpec,
        vintage: int,
        county_fips: str,
    ) -> bool:
        """
        Check if county-based data already exists for this vintage + county.

        Falls back to a vintage-only check if the table lacks statefp/countyfp
        columns (e.g. ADDR files where the county is only encoded in the
        filename, not the data).
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        statefp_col = "statefp" if spec.lowercase_columns else "STATEFP"
        countyfp_col = "countyfp" if spec.lowercase_columns else "COUNTYFP"

        has_valid_to = self._table_has_column(fqn, "valid_to")
        valid_to_clause = 'and "valid_to" is null' if has_valid_to else ""

        has_statefp = self._table_has_column(fqn, statefp_col)
        has_countyfp = self._table_has_column(fqn, countyfp_col)

        state = county_fips[:2]
        county = county_fips[2:]

        try:
            if has_statefp and has_countyfp:
                df = self.engine.query(
                    f"""
                    select 1 from {fqn}
                    where vintage = %(vintage)s
                      and "{statefp_col}" = %(state)s
                      and "{countyfp_col}" = %(county)s
                      {valid_to_clause}
                    limit 1
                    """,
                    {"vintage": vintage, "state": state, "county": county},
                )
            else:
                # No county columns in the data — best we can do is
                # check by vintage alone.
                df = self.engine.query(
                    f"""
                    select 1 from {fqn}
                    where vintage = %(vintage)s
                      {valid_to_clause}
                    limit 1
                    """,
                    {"vintage": vintage},
                )
            return not df.empty
        except Exception:
            return False

    def _table_has_column(self, fqn: str, column_name: str) -> bool:
        """Check if a table has a given column. Returns False if the table doesn't exist."""
        schema, table = fqn.split(".", 1)
        try:
            df = self.engine.query(
                """
                select 1 from information_schema.columns
                where table_schema = %(schema)s
                  and table_name = %(table)s
                  and column_name = %(column)s
                limit 1
                """,
                {"schema": schema, "table": table, "column": column_name},
            )
            return not df.empty
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _get_geometry_column(spec: TigerDatasetSpec) -> str:
        """Return the geometry column name for the target table."""
        return "geom"

    @staticmethod
    def _resolve_entity_key(
        spec: TigerDatasetSpec,
        columns: list[str],
    ) -> list[str] | None:
        """
        Resolve the entity key for a dataset.

        Priority:
        1. Explicit entity_key on the spec (always used if set).
        2. Auto-detect from parsed columns by checking for known
           TIGER ID column names (including year-suffixed variants
           like geoid20).
        3. None — no entity key, no SCD2.
        """
        if spec.entity_key is not None:
            return spec.entity_key

        col_names_lower = [c.lower() for c in columns]
        for candidate in _CANDIDATE_ID_COLUMNS:
            for col_lower, col_original in zip(col_names_lower, columns):
                if col_lower == candidate or col_lower.startswith(candidate):
                    col_name = col_lower if spec.lowercase_columns else col_original
                    return [col_name, "vintage"]

        return None

    @staticmethod
    def _extract_county_fips(filename: str, vintage: int) -> str | None:
        """
        Extract the 5-digit county FIPS from a county-based TIGER filename.

        County-based files are named: tl_{year}_{ssccc}_{layer}.zip
        where ss is state FIPS and ccc is county FIPS.
        """
        import re

        match = re.match(rf"tl_{vintage}_(\d{{5}})_", filename)
        return match.group(1) if match else None
