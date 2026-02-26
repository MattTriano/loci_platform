# """
# OSM data collection and ingestion pipeline.

# Downloads PBF extracts from Geofabrik, parses them via parse_pbf,
# and ingests to PostGIS via StagedIngest with SCD2 merge on [osm_id].

# Usage:
#     from loci.collectors.osm.metadata import OsmMetadata
#     from loci.collectors.osm.spec import OsmDatasetSpec
#     from loci.collectors.osm.collector import OsmCollector

#     spec = OsmDatasetSpec(
#         name="illinois_nodes",
#         region_id="illinois",
#         element_type="nodes",
#         target_table="osm_nodes",
#         target_schema="raw_data",
#     )

#     collector = OsmCollector(engine=engine)
#     collector.collect(spec)
# """

# from __future__ import annotations

# import logging
# import tempfile
# from pathlib import Path

# import requests
# from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
# from tenacity import (
#     before_sleep_log,
#     retry,
#     retry_if_exception_type,
#     stop_after_attempt,
#     wait_exponential,
# )

# from loci.collectors.osm.metadata import OsmMetadata
# from loci.collectors.osm.spec import OsmDatasetSpec

# logger = logging.getLogger(__name__)

# METADATA_COLUMNS: set[str] = {
#     "ingested_at",
#     "record_hash",
#     "valid_from",
#     "valid_to",
# }


# class OsmCollector:
#     """
#     Orchestrates downloading, parsing, and ingesting OSM PBF extracts.

#     Parameters
#     ----------
#     engine : PostgresEngine
#     tracker : IngestionTracker, optional
#         If provided, logs each collection run.
#     """

#     def __init__(self, engine, tracker=None):
#         self.engine = engine
#         self.tracker = tracker
#         self._metadata = OsmMetadata()
#         self._session = requests.Session()
#         self.logger = logging.getLogger("osm_collector")

#     def collect(self, spec: OsmDatasetSpec, force: bool = False) -> dict:
#         """
#         Collect and ingest OSM data defined by an OsmDatasetSpec.

#         Downloads the PBF file from Geofabrik (or the explicit pbf_url),
#         parses the specified element type, and ingests via StagedIngest
#         with SCD2 merge on [osm_id].

#         Parameters
#         ----------
#         spec : OsmDatasetSpec
#         force : bool
#             If True, skip idempotency check and re-ingest.

#         Returns a summary dict.
#         """
#         summary = {
#             "spec_name": spec.name,
#             "region_id": spec.region_id,
#             "element_type": spec.element_type,
#             "rows_staged": 0,
#             "rows_merged": 0,
#             "features_parsed": 0,
#             "features_failed": 0,
#             "errors": [],
#         }

#         if not force and self._already_ingested(spec):
#             self.logger.info(
#                 "Skipping %s (already ingested)",
#                 spec.name,
#             )
#             summary["skipped"] = True
#             return summary

#         # Resolve the PBF download URL
#         url = spec.pbf_url or self._metadata.get_download_url(spec.region_id)

#         try:
#             filepath = self._download_to_tempfile(url)
#         except Exception as e:
#             self.logger.error("Failed to download %s: %s", url, e)
#             summary["errors"].append({"stage": "download", "error": str(e)})
#             return summary

#         try:
#             staged, merged, parsed, failed = self._parse_and_ingest(
#                 spec, filepath, url
#             )
#             summary["rows_staged"] = staged
#             summary["rows_merged"] = merged
#             summary["features_parsed"] = parsed
#             summary["features_failed"] = failed
#         except Exception as e:
#             self.logger.error("Failed to ingest %s: %s", spec.name, e)
#             summary["errors"].append({"stage": "ingest", "error": str(e)})
#         finally:
#             filepath.unlink(missing_ok=True)
#             self.logger.info("Cleaned up temp file %s", filepath)

#         self.logger.info("Collection complete for %r: %s", spec.name, summary)
#         return summary

#     # ------------------------------------------------------------------ #
#     #  Core: parse → ingest
#     # ------------------------------------------------------------------ #

#     def _parse_and_ingest(
#         self,
#         spec: OsmDatasetSpec,
#         filepath: Path,
#         url: str,
#     ) -> tuple[int, int, int, int]:
#         """
#         Parse a PBF file and ingest into the target table.

#         Returns (rows_staged, rows_merged, features_parsed, features_failed).
#         """
#         from loci.parsers.pbf import parse_pbf

#         self.logger.info(
#             "Parsing %s from %s (element_type=%s)",
#             spec.name,
#             filepath,
#             spec.element_type,
#         )

#         batches, parse_result = parse_pbf(
#             filepath=filepath,
#             element_type=spec.element_type,
#             geometry_column=spec.geometry_column,
#             srid=spec.srid,
#             batch_size=spec.batch_size,
#             location_storage=spec.location_storage,
#         )

#         dataset_id = f"{spec.name}/{spec.region_id}"

#         if self.tracker:
#             with self.tracker.track(
#                 source="geofabrik",
#                 dataset_id=dataset_id,
#                 target_table=f"{spec.target_schema}.{spec.target_table}",
#                 metadata={
#                     "region_id": spec.region_id,
#                     "element_type": spec.element_type,
#                     "url": url,
#                 },
#             ) as run:
#                 staged, merged = self._ingest_batches(spec, batches)
#                 run.rows_staged = staged
#                 run.rows_merged = merged
#                 run.metadata["features_parsed"] = parse_result.features_parsed
#                 run.metadata["features_failed"] = parse_result.features_failed
#         else:
#             staged, merged = self._ingest_batches(spec, batches)

#         self.logger.info(
#             "Ingested %s: parsed=%d failed=%d staged=%d merged=%d",
#             spec.name,
#             parse_result.features_parsed,
#             parse_result.features_failed,
#             staged,
#             merged,
#         )
#         return staged, merged, parse_result.features_parsed, parse_result.features_failed

#     def _ingest_batches(
#         self,
#         spec: OsmDatasetSpec,
#         batches,
#     ) -> tuple[int, int]:
#         """
#         Feed parsed batches into StagedIngest with SCD2 merge.

#         Returns (rows_staged, rows_merged).
#         """
#         import time

#         with self.engine.staged_ingest(
#             target_table=spec.target_table,
#             target_schema=spec.target_schema,
#             entity_key=spec.entity_key,
#             metadata_columns=METADATA_COLUMNS,
#         ) as stager:
#             start = time.monotonic()
#             total_rows = 0
#             for batch in batches:
#                 stager.write_batch(batch)
#                 total_rows += len(batch)
#                 elapsed = time.monotonic() - start
#                 rate = total_rows / elapsed if elapsed > 0 else 0
#                 self.logger.info(
#                     "%s: %d rows ingested (%.0f rows/sec)",
#                     spec.name,
#                     total_rows,
#                     rate,
#                 )

#         return stager.rows_staged, stager.rows_merged

#     # ------------------------------------------------------------------ #
#     #  Download
#     # ------------------------------------------------------------------ #

#     @retry(
#         retry=retry_if_exception_type(
#             (ChunkedEncodingError, ConnectionError, ReadTimeout),
#         ),
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=10, max=120),
#         before_sleep=before_sleep_log(logger, logging.WARNING),
#     )
#     def _download_to_tempfile(self, url: str) -> Path:
#         """Download a URL to a temp file. Returns the file path."""
#         self.logger.info("Downloading %s", url)
#         resp = self._session.get(url, stream=True, timeout=300)
#         resp.raise_for_status()

#         tmp = tempfile.NamedTemporaryFile(
#             suffix=".osm.pbf",
#             prefix="osm_",
#             delete=False,
#         )
#         try:
#             for chunk in resp.iter_content(chunk_size=8192):
#                 tmp.write(chunk)
#             tmp.close()
#             filepath = Path(tmp.name)
#             size_mb = filepath.stat().st_size / (1024 * 1024)
#             self.logger.info(
#                 "Downloaded %s to %s (%.1f MB)",
#                 url,
#                 filepath,
#                 size_mb,
#             )
#             return filepath
#         except Exception:
#             tmp.close()
#             Path(tmp.name).unlink(missing_ok=True)
#             raise

#     # ------------------------------------------------------------------ #
#     #  Idempotency
#     # ------------------------------------------------------------------ #

#     def _already_ingested(self, spec: OsmDatasetSpec) -> bool:
#         """
#         Check if current (non-superseded) data exists for this spec.

#         Since OSM snapshots have no vintage, we just check whether
#         any current rows exist in the target table.
#         """
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         try:
#             df = self.engine.query(
#                 f"""
#                 select 1 from {fqn}
#                 where "valid_to" is null
#                 limit 1
#                 """,
#             )
#             return not df.empty
#         except Exception:
#             # Table might not exist yet
#             return False

# """
# OSM data collection and ingestion pipeline.

# Downloads PBF extracts from Geofabrik, parses them via parse_pbf,
# and ingests to PostGIS via StagedIngest with SCD2 merge on
# [osm_id, region_id].

# Usage:
#     from loci.collectors.osm.metadata import OsmMetadata
#     from loci.collectors.osm.spec import OsmDatasetSpec
#     from loci.collectors.osm.collector import OsmCollector

#     spec = OsmDatasetSpec(
#         name="osm_nodes",
#         region_ids=["us/illinois", "us/indiana"],
#         element_type="nodes",
#         target_table="osm_nodes",
#         target_schema="raw_data",
#     )

#     collector = OsmCollector(engine=engine)
#     collector.collect(spec)
# """


##################################################################################
##################################################################################
##################################################################################
#                                     V2                                         #
##################################################################################
##################################################################################
##################################################################################

# from __future__ import annotations

# import logging
# import tempfile
# import time
# from pathlib import Path

# import requests
# from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
# from tenacity import (
#     before_sleep_log,
#     retry,
#     retry_if_exception_type,
#     stop_after_attempt,
#     wait_exponential,
# )

# from loci.collectors.osm.metadata import OsmMetadata
# from loci.collectors.osm.spec import OsmDatasetSpec

# logger = logging.getLogger(__name__)

# METADATA_COLUMNS: set[str] = {
#     "ingested_at",
#     "record_hash",
#     "valid_from",
#     "valid_to",
# }


# class OsmCollector:
#     """
#     Orchestrates downloading, parsing, and ingesting OSM PBF extracts.

#     Parameters
#     ----------
#     engine : PostgresEngine
#     tracker : IngestionTracker, optional
#         If provided, logs each collection run.
#     """

#     def __init__(self, engine, tracker=None):
#         self.engine = engine
#         self.tracker = tracker
#         self._metadata = OsmMetadata()
#         self._session = requests.Session()
#         self.logger = logging.getLogger("osm_collector")

#     def collect(self, spec: OsmDatasetSpec, force: bool = False) -> dict:
#         """
#         Collect and ingest OSM data defined by an OsmDatasetSpec.

#         Iterates over region_ids, downloading and ingesting each one.

#         Parameters
#         ----------
#         spec : OsmDatasetSpec
#         force : bool
#             If True, skip idempotency checks and re-ingest.

#         Returns a summary dict.
#         """
#         summary = {
#             "spec_name": spec.name,
#             "element_type": spec.element_type,
#             "regions_processed": 0,
#             "regions_skipped": 0,
#             "total_rows_staged": 0,
#             "total_rows_merged": 0,
#             "total_features_parsed": 0,
#             "total_features_failed": 0,
#             "errors": [],
#         }

#         for region_id in spec.region_ids:
#             if not force and self._already_ingested(spec, region_id):
#                 self.logger.info(
#                     "Skipping %s region=%s (already ingested)",
#                     spec.name,
#                     region_id,
#                 )
#                 summary["regions_skipped"] += 1
#                 continue

#             url = self._metadata.get_download_url(region_id)

#             try:
#                 filepath = self._download_to_tempfile(url)
#             except Exception as e:
#                 self.logger.error(
#                     "Failed to download %s for region=%s: %s",
#                     url,
#                     region_id,
#                     e,
#                 )
#                 summary["errors"].append(
#                     {"region_id": region_id, "stage": "download", "error": str(e)}
#                 )
#                 continue

#             try:
#                 staged, merged, parsed, failed = self._parse_and_ingest(
#                     spec, filepath, url, region_id
#                 )
#                 summary["regions_processed"] += 1
#                 summary["total_rows_staged"] += staged
#                 summary["total_rows_merged"] += merged
#                 summary["total_features_parsed"] += parsed
#                 summary["total_features_failed"] += failed
#             except Exception as e:
#                 self.logger.error(
#                     "Failed to ingest %s region=%s: %s",
#                     spec.name,
#                     region_id,
#                     e,
#                 )
#                 summary["errors"].append(
#                     {"region_id": region_id, "stage": "ingest", "error": str(e)}
#                 )
#             finally:
#                 filepath.unlink(missing_ok=True)
#                 self.logger.info("Cleaned up temp file %s", filepath)

#         self.logger.info("Collection complete for %r: %s", spec.name, summary)
#         return summary

#     # ------------------------------------------------------------------ #
#     #  Core: parse → ingest
#     # ------------------------------------------------------------------ #

#     def _parse_and_ingest(
#         self,
#         spec: OsmDatasetSpec,
#         filepath: Path,
#         url: str,
#         region_id: str,
#     ) -> tuple[int, int, int, int]:
#         """
#         Parse a PBF file and ingest into the target table.

#         Returns (rows_staged, rows_merged, features_parsed, features_failed).
#         """
#         from loci.parsers.pbf import parse_pbf

#         self.logger.info(
#             "Parsing %s region=%s from %s (element_type=%s)",
#             spec.name,
#             region_id,
#             filepath,
#             spec.element_type,
#         )

#         batches, parse_result = parse_pbf(
#             filepath=filepath,
#             element_type=spec.element_type,
#             geometry_column=spec.geometry_column,
#             srid=spec.srid,
#             batch_size=spec.batch_size,
#             location_storage=spec.location_storage,
#         )

#         dataset_id = f"{spec.name}/{region_id}"

#         if self.tracker:
#             with self.tracker.track(
#                 source="geofabrik",
#                 dataset_id=dataset_id,
#                 target_table=f"{spec.target_schema}.{spec.target_table}",
#                 metadata={
#                     "region_id": region_id,
#                     "element_type": spec.element_type,
#                     "url": url,
#                 },
#             ) as run:
#                 staged, merged = self._ingest_batches(spec, batches, region_id)
#                 run.rows_staged = staged
#                 run.rows_merged = merged
#                 run.metadata["features_parsed"] = parse_result.features_parsed
#                 run.metadata["features_failed"] = parse_result.features_failed
#         else:
#             staged, merged = self._ingest_batches(spec, batches, region_id)

#         self.logger.info(
#             "Ingested %s region=%s: parsed=%d failed=%d staged=%d merged=%d",
#             spec.name,
#             region_id,
#             parse_result.features_parsed,
#             parse_result.features_failed,
#             staged,
#             merged,
#         )
#         return staged, merged, parse_result.features_parsed, parse_result.features_failed

#     def _ingest_batches(
#         self,
#         spec: OsmDatasetSpec,
#         batches,
#         region_id: str,
#     ) -> tuple[int, int]:
#         """
#         Feed parsed batches into StagedIngest with SCD2 merge.

#         Adds a region_id column to each row.

#         Returns (rows_staged, rows_merged).
#         """
#         with self.engine.staged_ingest(
#             target_table=spec.target_table,
#             target_schema=spec.target_schema,
#             entity_key=spec.entity_key,
#             metadata_columns=METADATA_COLUMNS,
#         ) as stager:
#             start = time.monotonic()
#             total_rows = 0
#             for batch in batches:
#                 for row in batch:
#                     row["region_id"] = region_id
#                 stager.write_batch(batch)
#                 total_rows += len(batch)
#                 elapsed = time.monotonic() - start
#                 rate = total_rows / elapsed if elapsed > 0 else 0
#                 if total_rows % 500_000 < len(batch):
#                     self.logger.info(
#                         "%s region=%s: %d rows staged (%.0f rows/sec)",
#                         spec.name,
#                         region_id,
#                         total_rows,
#                         rate,
#                     )

#         return stager.rows_staged, stager.rows_merged

#     # ------------------------------------------------------------------ #
#     #  Download
#     # ------------------------------------------------------------------ #

#     @retry(
#         retry=retry_if_exception_type(
#             (ChunkedEncodingError, ConnectionError, ReadTimeout),
#         ),
#         stop=stop_after_attempt(3),
#         wait=wait_exponential(multiplier=10, max=120),
#         before_sleep=before_sleep_log(logger, logging.WARNING),
#     )
#     def _download_to_tempfile(self, url: str) -> Path:
#         """Download a URL to a temp file. Returns the file path."""
#         self.logger.info("Downloading %s", url)
#         resp = self._session.get(url, stream=True, timeout=300)
#         resp.raise_for_status()

#         tmp = tempfile.NamedTemporaryFile(
#             suffix=".osm.pbf",
#             prefix="osm_",
#             delete=False,
#         )
#         try:
#             for chunk in resp.iter_content(chunk_size=8192):
#                 tmp.write(chunk)
#             tmp.close()
#             filepath = Path(tmp.name)
#             size_mb = filepath.stat().st_size / (1024 * 1024)
#             self.logger.info(
#                 "Downloaded %s to %s (%.1f MB)",
#                 url,
#                 filepath,
#                 size_mb,
#             )
#             return filepath
#         except Exception:
#             tmp.close()
#             Path(tmp.name).unlink(missing_ok=True)
#             raise

#     # ------------------------------------------------------------------ #
#     #  Idempotency
#     # ------------------------------------------------------------------ #

#     def _already_ingested(self, spec: OsmDatasetSpec, region_id: str) -> bool:
#         """
#         Check if current (non-superseded) data exists for this region.
#         """
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         try:
#             df = self.engine.query(
#                 f"""
#                 select 1 from {fqn}
#                 where "region_id" = %(region_id)s
#                   and "valid_to" is null
#                 limit 1
#                 """,
#                 {"region_id": region_id},
#             )
#             return not df.empty
#         except Exception:
#             return False


"""
OSM data collection and ingestion pipeline.

Downloads PBF extracts from Geofabrik, parses them via parse_pbf,
and ingests to PostGIS via StagedIngest with SCD2 merge on
[osm_id, region_id].

Usage:
    from loci.collectors.osm.metadata import OsmMetadata
    from loci.collectors.osm.spec import OsmDatasetSpec
    from loci.collectors.osm.collector import OsmCollector

    spec = OsmDatasetSpec(
        name="osm_nodes",
        region_ids=["us/illinois", "us/indiana"],
        element_type="nodes",
        target_table="osm_nodes",
        target_schema="raw_data",
    )

    collector = OsmCollector(engine=engine)
    collector.collect(spec)
"""

from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path

import requests
from loci.collectors.osm.metadata import OsmMetadata
from loci.collectors.osm.spec import OsmDatasetSpec
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
    "osm_timestamp",
    "osm_version",
    "osm_changeset",
}


class OsmCollector:
    """
    Orchestrates downloading, parsing, and ingesting OSM PBF extracts.

    Parameters
    ----------
    engine : PostgresEngine
    tracker : IngestionTracker, optional
        If provided, logs each collection run.
    """

    def __init__(self, engine, tracker=None):
        self.engine = engine
        self.tracker = tracker
        self._metadata = OsmMetadata()
        self._session = requests.Session()
        self.logger = logging.getLogger("osm_collector")

    def collect(self, spec: OsmDatasetSpec, force: bool = False) -> dict:
        """
        Collect and ingest OSM data defined by an OsmDatasetSpec.

        Iterates over region_ids, downloading and ingesting each one.

        Parameters
        ----------
        spec : OsmDatasetSpec
        force : bool
            If True, skip idempotency checks and re-ingest.

        Returns a summary dict.
        """
        summary = {
            "spec_name": spec.name,
            "element_type": spec.element_type,
            "regions_processed": 0,
            "regions_skipped": 0,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "total_features_parsed": 0,
            "total_features_failed": 0,
            "errors": [],
        }

        for region_id in spec.region_ids:
            if not force and self._already_ingested(spec, region_id):
                self.logger.info(
                    "Skipping %s region=%s (already ingested)",
                    spec.name,
                    region_id,
                )
                summary["regions_skipped"] += 1
                continue

            url = self._metadata.get_download_url(region_id)

            try:
                filepath = self._download_to_tempfile(url)
            except Exception as e:
                self.logger.error(
                    "Failed to download %s for region=%s: %s",
                    url,
                    region_id,
                    e,
                )
                summary["errors"].append(
                    {"region_id": region_id, "stage": "download", "error": str(e)}
                )
                continue

            try:
                staged, merged, parsed, failed = self._parse_and_ingest(
                    spec, filepath, url, region_id
                )
                summary["regions_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
                summary["total_features_parsed"] += parsed
                summary["total_features_failed"] += failed
            except Exception as e:
                self.logger.error(
                    "Failed to ingest %s region=%s: %s",
                    spec.name,
                    region_id,
                    e,
                )
                summary["errors"].append(
                    {"region_id": region_id, "stage": "ingest", "error": str(e)}
                )
            finally:
                filepath.unlink(missing_ok=True)
                self.logger.info("Cleaned up temp file %s", filepath)

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    # ------------------------------------------------------------------ #
    #  Core: parse → ingest
    # ------------------------------------------------------------------ #

    def _parse_and_ingest(
        self,
        spec: OsmDatasetSpec,
        filepath: Path,
        url: str,
        region_id: str,
    ) -> tuple[int, int, int, int]:
        """
        Parse a PBF file and ingest into the target table.

        Returns (rows_staged, rows_merged, features_parsed, features_failed).
        """
        from loci.parsers.pbf import parse_pbf

        self.logger.info(
            "Parsing %s region=%s from %s (element_type=%s)",
            spec.name,
            region_id,
            filepath,
            spec.element_type,
        )

        batches, parse_result = parse_pbf(
            filepath=filepath,
            element_type=spec.element_type,
            geometry_column=spec.geometry_column,
            srid=spec.srid,
            batch_size=spec.batch_size,
            location_storage=spec.location_storage,
        )

        dataset_id = f"{spec.name}/{region_id}"

        if self.tracker:
            with self.tracker.track(
                source="geofabrik",
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table}",
                metadata={
                    "region_id": region_id,
                    "element_type": spec.element_type,
                    "url": url,
                },
            ) as run:
                staged, merged = self._ingest_batches(spec, batches, region_id)
                run.rows_staged = staged
                run.rows_merged = merged
                run.metadata["features_parsed"] = parse_result.features_parsed
                run.metadata["features_failed"] = parse_result.features_failed
        else:
            staged, merged = self._ingest_batches(spec, batches, region_id)

        self.logger.info(
            "Ingested %s region=%s: parsed=%d failed=%d staged=%d merged=%d",
            spec.name,
            region_id,
            parse_result.features_parsed,
            parse_result.features_failed,
            staged,
            merged,
        )
        return staged, merged, parse_result.features_parsed, parse_result.features_failed

    def _ingest_batches(
        self,
        spec: OsmDatasetSpec,
        batches,
        region_id: str,
    ) -> tuple[int, int]:
        """
        Feed parsed batches into StagedIngest with SCD2 merge.

        Adds a region_id column to each row.

        Returns (rows_staged, rows_merged).
        """
        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=METADATA_COLUMNS,
        ) as stager:
            start = time.monotonic()
            total_rows = 0
            for batch in batches:
                for row in batch:
                    row["region_id"] = region_id
                stager.write_batch(batch)
                total_rows += len(batch)
                elapsed = time.monotonic() - start
                rate = total_rows / elapsed if elapsed > 0 else 0
                if total_rows % 500_000 < len(batch):
                    self.logger.info(
                        "%s region=%s: %d rows staged (%.0f rows/sec)",
                        spec.name,
                        region_id,
                        total_rows,
                        rate,
                    )

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
            suffix=".osm.pbf",
            prefix="osm_",
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

    def _already_ingested(self, spec: OsmDatasetSpec, region_id: str) -> bool:
        """
        Check if current (non-superseded) data exists for this region.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select 1 from {fqn}
                where "region_id" = %(region_id)s
                  and "valid_to" is null
                limit 1
                """,
                {"region_id": region_id},
            )
            return not df.empty
        except Exception:
            return False
