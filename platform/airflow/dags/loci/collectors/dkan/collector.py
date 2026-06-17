# /loci_platform/platform/airflow/dags/loci/collectors/dkan/collector.py
"""
DKANCollector — orchestrates collection of DKAN-portal datasets.

One collector instance serves every DKAN portal: clients are cached per
spec.base_url (the CKANCollector pattern). Known CMS portals are the
Provider Data Catalog (data.cms.gov/provider-data) and Open Payments
(openpaymentsdata.cms.gov).

Update scheme:
    The unit of work is one metastore dataset. DKAN datasets are
    refreshed in place — one identifier, a dataset-level modified date,
    no version array — so collect(spec, force=False) recollects a
    dataset when it is missing, incomplete, or its modified date has
    advanced; force=True recollects everything. Ingestion is
    StagedIngest in SCD2 mode, so recollection is always safe.

    A spec may carry several dataset identifiers (e.g. one Open
    Payments dataset per program year) landing in one target table;
    each is checked and collected independently. Every row is stamped
    with _source_dataset (which dataset it came from — the freshness
    check's grouping key) and _source_modified (the dataset's modified
    date at collection time); both are hash-excluded metadata.

Freshness check (per dataset, same scheme as CMSCollector):
    fresh = (current target rows for the dataset >= datastore count)
        and (stored max _source_modified >= catalog modified)
    After each successful ingest, _source_modified is advanced on the
    dataset's current rows so re-publications whose rows dedupe away
    don't trigger perpetual re-downloads.

Column normalization:
    DKAN's datastore serves normalized column names (lowercase,
    whitespace -> underscore, other punctuation DROPPED — "County/
    Parish" becomes "countyparish"), while distribution files keep the
    original headers. To keep the two retrieval modes hash-consistent,
    file headers are normalized with the same rule. Names are also
    truncated to Postgres's 63-character identifier limit (Open
    Payments has a 64-character column) with collision suffixing.

Retrieval (driven by the spec):
    spec.retrieval == "datastore": page rows out of the datastore API
        (500 rows/page cap)
    spec.retrieval == "file": download the distribution file and parse
        it with the streaming CSV parser

invalidate_missing (spec flag, default False):
    For single-dataset refresh-in-place specs, True closes out current
    rows whose entity is absent from the fresh pull (delisted
    entities). The spec forbids it for multi-dataset families.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from loci.collectors.dkan.client import DKANClient
from loci.collectors.dkan.metadata import DKANMetadata
from loci.collectors.dkan.spec import DKANDatasetSpec
from loci.parsers.csv_parser import parse_csv
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)

PG_MAX_IDENTIFIER = 63


class DKANCollector:
    """
    Orchestrates collecting and ingesting DKAN datasets defined by a
    DKANDatasetSpec.

    Parameters
    ----------
    engine : PostgresEngine
    tracker : IngestionTracker, optional
        If provided (or by default), logs each dataset run.
    clients : dict[str, DKANClient], optional
        Pre-configured clients keyed by portal base_url. Portals not in
        the mapping get a default client on first use. Useful for
        custom client settings and for injecting fakes in tests.
    """

    SOURCE_NAME = "dkan"
    # Written by the pipeline, never present in source rows; excluded
    # from the staging column list by StagedIngest.
    PIPELINE_COLUMNS = {"ingested_at", "record_hash", "valid_from", "valid_to"}
    # Written by this collector, but excluded from the record hash so
    # re-publications and provenance don't create new versions.
    SOURCE_METADATA_COLUMNS = {"_source_dataset", "_source_modified"}

    def __init__(
        self,
        engine: Any,
        tracker: IngestionTracker | None = None,
        logger: logging.Logger | None = None,
        clients: dict[str, DKANClient] | None = None,
    ) -> None:
        self.engine = engine
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.logger = logger or logging.getLogger("dkan_collector")
        self._client_cache: dict[str, DKANClient] = {
            url.rstrip("/"): client for url, client in (clients or {}).items()
        }

    def _get_client(self, base_url: str) -> DKANClient:
        """Return a cached DKANClient for the given portal URL."""
        base_url = base_url.rstrip("/")
        if base_url not in self._client_cache:
            self._client_cache[base_url] = DKANClient(base_url)
        return self._client_cache[base_url]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: DKANDatasetSpec, force: bool = False) -> dict[str, Any]:
        """
        Collect and ingest all datasets defined by a DKANDatasetSpec.

        Parameters
        ----------
        spec : DKANDatasetSpec
        force : bool
            If True, skip freshness checks and recollect every dataset.

        Returns a summary dict with counts.
        """
        client = self._get_client(spec.base_url)

        summary: dict[str, Any] = {
            "spec_name": spec.name,
            "datasets_processed": 0,
            "datasets_skipped": 0,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "total_rows_invalidated": 0,
            "errors": [],
        }

        for identifier in spec.dataset_identifiers:
            try:
                dataset = client.get_dataset(identifier)
                modified = dataset.get("modified")

                if not force and self._already_ingested(spec, client, identifier, modified):
                    self.logger.info(
                        "Skipping %s dataset=%s (already ingested, source unchanged)",
                        spec.name,
                        identifier,
                    )
                    summary["datasets_skipped"] += 1
                    continue

                staged, merged, invalidated = self._collect_dataset(
                    spec, client, identifier, dataset
                )
                summary["datasets_processed"] += 1
                summary["total_rows_staged"] += staged
                summary["total_rows_merged"] += merged
                summary["total_rows_invalidated"] += invalidated
            except Exception as e:
                self.logger.error("Failed: %s dataset=%s: %s", spec.name, identifier, e)
                summary["errors"].append({"dataset_identifier": identifier, "error": str(e)})

        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    def generate_ddl(self, spec: DKANDatasetSpec) -> str:
        """
        Generate a CREATE TABLE statement for a DKANDatasetSpec.

        Samples one datastore row from every dataset in the spec and
        unions the normalized column sets, so a multi-dataset family's
        column drift across siblings all gets a home. All source
        columns are text; casting is a downstream concern.
        """
        client = self._get_client(spec.base_url)
        metadata = DKANMetadata(client)

        columns: list[str] = []
        seen: set[str] = set()
        for identifier in spec.dataset_identifiers:
            raw = metadata.columns(identifier)
            if not raw:
                raise ValueError(
                    f"Dataset {identifier} returned no datastore sample; "
                    f"generate_ddl needs a datastore-backed dataset."
                )
            for col in self._build_name_map(raw).values():
                if col not in seen:
                    seen.add(col)
                    columns.append(col)

        fqn = f"{spec.target_schema}.{spec.target_table}"
        lines = [f"create table {fqn} ("]
        for col in columns:
            lines.append(f'    "{col}" text,')
        lines.append('    "_source_dataset" text not null,')
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

    def print_ddl(self, spec: DKANDatasetSpec) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        print(self.generate_ddl(spec))

    # ------------------------------------------------------------------
    # Freshness
    # ------------------------------------------------------------------

    def _already_ingested(
        self, spec: DKANDatasetSpec, client: DKANClient, identifier: str, modified: str | None
    ) -> bool:
        """
        Return True if this dataset is fully ingested and the source
        hasn't been re-published since. See the module docstring.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select count(*) as n, max("_source_modified") as max_mod
                from {fqn}
                where "_source_dataset" = %(identifier)s
                  and "valid_to" is null
                """,
                {"identifier": identifier},
            )
        except Exception:
            return False  # Table might not exist yet on first run

        n = int(df["n"].iloc[0])
        max_mod = df["max_mod"].iloc[0]
        if n == 0 or max_mod is None:
            return False

        # ISO date strings compare correctly as text.
        if modified and max_mod < modified:
            self.logger.info(
                "%s dataset=%s was re-published (stored %s < catalog %s)",
                spec.name,
                identifier,
                max_mod,
                modified,
            )
            return False

        source_count = client.row_count(identifier)
        if n < source_count:
            self.logger.info(
                "%s dataset=%s is incomplete (%d of %d rows)",
                spec.name,
                identifier,
                n,
                source_count,
            )
            return False

        return True

    # ------------------------------------------------------------------
    # Collection
    # ------------------------------------------------------------------

    def _collect_dataset(
        self, spec: DKANDatasetSpec, client: DKANClient, identifier: str, dataset: dict
    ) -> tuple[int, int, int]:
        """
        Fetch and ingest one dataset. Returns
        (rows_staged, rows_merged, rows_invalidated).
        """
        modified = dataset.get("modified")
        self.logger.info("Collecting %s dataset=%s via %s", spec.name, identifier, spec.retrieval)

        fqn = f"{spec.target_schema}.{spec.target_table}"
        table_columns = self._get_table_columns(spec.target_table, spec.target_schema)
        if not table_columns:
            raise RuntimeError(
                f"Target table {fqn} has no columns (does it exist?). "
                f"Run collector.print_ddl(spec) and create it first."
            )

        run_metadata = {
            "base_url": spec.base_url,
            "dataset_identifier": identifier,
            "source_modified": modified,
            "retrieval": spec.retrieval,
            "invalidate_missing": spec.invalidate_missing,
        }
        dataset_id = f"{spec.base_url}/{identifier}"

        with self.tracker.track(self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata) as run:
            with self.engine.staged_ingest(
                target_table=spec.target_table,
                target_schema=spec.target_schema,
                entity_key=spec.entity_key,
                metadata_columns=self.PIPELINE_COLUMNS,
                hash_exclude_columns=self.SOURCE_METADATA_COLUMNS,
                invalidate_missing=spec.invalidate_missing,
            ) as stager:
                name_map: dict[str, str] | None = None
                for batch in self._iter_batches(spec, client, identifier, dataset):
                    if name_map is None and batch:
                        name_map = self._build_name_map(list(batch[0].keys()))
                        self._warn_on_column_drift(set(name_map.values()), table_columns, fqn)
                    batch = self._prepare_batch(batch, name_map or {}, identifier, modified)
                    batch = self._filter_to_table_columns(batch, table_columns)
                    stager.write_batch(batch)

            run.rows_staged = stager.rows_staged
            run.rows_merged = stager.rows_merged
            run.rows_ingested = stager.rows_merged
            run.high_water_mark = modified

            if stager.rows_staged == 0:
                self.logger.warning("No rows returned for %s dataset=%s", spec.name, identifier)
            else:
                self._advance_source_modified(spec, identifier, modified)

        self.logger.info(
            "Ingested %s dataset=%s: staged=%d merged=%d invalidated=%d",
            spec.name,
            identifier,
            stager.rows_staged,
            stager.rows_merged,
            stager.rows_invalidated,
        )
        return stager.rows_staged, stager.rows_merged, stager.rows_invalidated

    def _iter_batches(
        self, spec: DKANDatasetSpec, client: DKANClient, identifier: str, dataset: dict
    ):
        """Yield batches of raw source rows via the spec's retrieval mode."""
        if spec.retrieval == "datastore":
            yield from client.iter_pages(identifier)
            return

        distributions = DKANMetadata.distributions(dataset)
        download_url = distributions[0].download_url if distributions else None
        if not download_url:
            raise ValueError(
                f"Dataset {identifier} has no distribution download URL; "
                f"set retrieval='datastore' on the spec."
            )
        filepath = client.download_to_tempfile(download_url, suffix=".csv")
        try:
            yield from parse_csv(filepath)
        finally:
            filepath.unlink(missing_ok=True)

    def _prepare_batch(
        self,
        batch: list[dict[str, Any]],
        name_map: dict[str, str],
        identifier: str,
        modified: str | None,
    ) -> list[dict[str, Any]]:
        """Normalize column names and stamp the provenance columns."""
        prepared = []
        for row in batch:
            row = {name_map[k]: v for k, v in row.items() if k in name_map}
            row["_source_dataset"] = identifier
            row["_source_modified"] = modified
            prepared.append(row)
        return prepared

    def _advance_source_modified(
        self, spec: DKANDatasetSpec, identifier: str, modified: str | None
    ) -> None:
        """
        Advance _source_modified on the dataset's current rows.

        Rows whose content didn't change in a re-publication are deduped
        by StagedIngest and keep their old stamp; without this update
        the freshness check would re-download the dataset on every run.
        """
        if not modified:
            return
        fqn = f"{spec.target_schema}.{spec.target_table}"
        self.engine.execute(
            f"""
            update {fqn}
            set "_source_modified" = %(modified)s
            where "_source_dataset" = %(identifier)s
              and "valid_to" is null
              and ("_source_modified" is null or "_source_modified" < %(modified)s)
            """,
            {"modified": modified, "identifier": identifier},
        )

    # ------------------------------------------------------------------
    # Column normalization (DKAN-matching)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_name(raw: str) -> str:
        """
        Normalize a column name the way DKAN's datastore does: lowercase,
        whitespace -> underscore, other punctuation DROPPED (so
        "County/Parish" -> "countyparish", matching the datastore), then
        truncated to Postgres's identifier limit.
        """
        name = raw.strip().lstrip("\ufeff").lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^a-z0-9_]", "", name)
        return name[:PG_MAX_IDENTIFIER].rstrip("_")

    def _build_name_map(self, raw_names: list[str]) -> dict[str, str]:
        """
        Ordered raw -> normalized mapping with collision handling
        (adapted from CKANCollector). Collisions — including ones
        created by 63-char truncation — are suffixed _2, _3, ...;
        empty normalized names are skipped.
        """
        mapping: dict[str, str] = {}
        taken: dict[str, list[str]] = {}

        for raw in raw_names:
            base = self._normalize_name(raw)
            if not base:
                continue
            if base not in taken:
                taken[base] = [raw]
                mapping[raw] = base
                continue
            taken[base].append(raw)
            suffix = len(taken[base])
            while True:
                tail = f"_{suffix}"
                candidate = base[: PG_MAX_IDENTIFIER - len(tail)] + tail
                if candidate not in taken:
                    break
                suffix += 1
            taken[candidate] = [raw]
            mapping[raw] = candidate

        collisions = {n: rs for n, rs in taken.items() if len(rs) > 1}
        if collisions:
            self.logger.warning("Column name collisions after normalization: %s", collisions)
        return mapping

    # ------------------------------------------------------------------
    # Table / column helpers
    # ------------------------------------------------------------------

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
        self, source_columns: set[str], table_columns: set[str], fqn: str
    ) -> None:
        """Warn-only comparison of incoming columns vs the target table."""
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
                "Columns in %s but not in this dataset's source: %s",
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
