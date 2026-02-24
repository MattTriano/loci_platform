from __future__ import annotations

import json
import logging
import tempfile
from datetime import UTC
from pathlib import Path
from typing import Any

from loci.collectors.config import IncrementalConfig
from loci.collectors.exceptions import SchemaDriftError
from loci.collectors.socrata.client import SocrataClient
from loci.collectors.socrata.metadata import SocrataTableMetadata
from loci.parsers.csv_parser import parse_csv
from loci.parsers.geojson import parse_geojson
from loci.sources.update_configs import DatasetUpdateConfig
from loci.tracking.ingestion_tracker import IngestionTracker
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class SocrataCollector:
    """
    High-level orchestrator for Socrata dataset ingestion.
    """

    SOURCE_NAME = "socrata"
    SYSTEM_FIELD_RENAMES = {
        ":id": "socrata_id",
        ":updated_at": "socrata_updated_at",
        ":created_at": "socrata_created_at",
        ":version": "socrata_version",
    }
    METADATA_COLUMNS = {
        "socrata_id",
        "socrata_updated_at",
        "socrata_created_at",
        "socrata_version",
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }

    def __init__(
        self,
        engine: Any,
        tracker: IngestionTracker | None = None,
        app_token: str | None = None,
        page_size: int = 25000,
    ) -> None:
        self.engine = engine
        self.tracker = tracker or IngestionTracker(engine=engine)
        self.app_token = app_token
        self.page_size = page_size
        self.logger = logging.getLogger("socrata_collector")

        self._client: SocrataClient | None = None
        self._metadata_cache: dict[str, SocrataTableMetadata] = {}

    @property
    def client(self) -> SocrataClient:
        if self._client is None:
            self._client = SocrataClient(
                app_token=self.app_token,
                page_size=self.page_size,
            )
        return self._client

    @staticmethod
    def _add_system_field_defaults(
        rows: list[dict[str, Any]], timestamp: str
    ) -> list[dict[str, Any]]:
        """Add default values for Socrata system fields not present in file exports."""
        for row in rows:
            row.setdefault("socrata_updated_at", timestamp)
            row.setdefault("socrata_created_at", None)
            row.setdefault("socrata_id", None)
            row.setdefault("socrata_version", None)
        return rows

    def _get_metadata(self, dataset_id: str) -> SocrataTableMetadata:
        if dataset_id not in self._metadata_cache:
            self._metadata_cache[dataset_id] = SocrataTableMetadata(dataset_id)
        return self._metadata_cache[dataset_id]

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

    def _filter_to_table_columns(
        self, rows: list[dict[str, Any]], table_columns: set[str]
    ) -> list[dict[str, Any]]:
        dropped = set()
        for row in rows:
            extra = set(row.keys()) - table_columns
            for k in extra:
                del row[k]
            dropped.update(extra)
        if dropped:
            self.logger.warning("Dropped columns not in target table: %s", dropped)
        return rows

    def full_refresh(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        config: DatasetUpdateConfig | None = None,
    ) -> int:
        """
        Dispatch a full refresh based on config.full_update_mode.

        Args:
            dataset_id:     Socrata 4x4 identifier.
            target_table:   Target table name.
            target_schema:  Target schema name.
            config:         DatasetUpdateConfig. If None, defaults to API mode.

        Returns:
            Number of rows merged.
        """
        mode = config.full_update_mode if config else "api"
        entity_key = config.entity_key if config else None

        if mode == "file_download":
            return self.full_refresh_via_file(
                dataset_id=dataset_id,
                target_table=target_table,
                target_schema=target_schema,
                entity_key=entity_key,
            )
        else:
            return self.full_refresh_via_api(
                dataset_id=dataset_id,
                target_table=target_table,
                target_schema=target_schema,
                entity_key=entity_key,
            )

    def full_refresh_via_file(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        entity_key: list[str] | None = None,
    ) -> int:
        """
        Full refresh by downloading the dataset export file (CSV or GeoJSON),
        parsing it with a streaming parser, and ingesting via staged_ingest.

        System fields (socrata_updated_at) are set to the current timestamp
        since they are not available in bulk exports.
        """
        from datetime import datetime

        meta = self._get_metadata(dataset_id)
        download_url = meta.data_download_url
        download_format = meta.download_format
        fqn = f"{target_schema}.{target_table}"

        run_metadata = {
            "mode": "full_refresh_file",
            "download_format": download_format,
            "download_url": download_url,
            "entity_key": entity_key,
            "domain": meta.domain,
        }

        self.logger.info(
            "Starting file download full refresh for %s (%s format)",
            dataset_id,
            download_format,
        )

        with self.tracker.track(self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata) as run:
            filepath = self._download_to_tempfile(download_url, download_format)

            try:
                now_utc = datetime.now(UTC).isoformat()
                geojson_result = None

                if download_format == "GeoJSON":
                    geometry_column = self.engine._get_geometry_column(target_table, target_schema)
                    batches, geojson_result = parse_geojson(
                        filepath,
                        geometry_column=geometry_column,
                    )
                else:
                    batches = parse_csv(filepath)

                location_columns = self._get_location_columns(dataset_id)
                table_columns = self._get_table_columns(target_table, target_schema)
                self.logger.info("Table columns: %s ", table_columns)
                first_batch = True
                with self.engine.staged_ingest(
                    target_table=target_table,
                    target_schema=target_schema,
                    entity_key=entity_key,
                    metadata_columns=self.METADATA_COLUMNS,
                ) as stager:
                    for batch in batches:
                        batch = self._rename_file_columns(batch, dataset_id)
                        batch = self._add_system_field_defaults(batch, now_utc)
                        batch = self._filter_to_table_columns(batch, table_columns)
                        if first_batch:
                            self.logger.info("First batch cols: %s ", batch[0])
                            first_batch = False
                        if location_columns:
                            self._convert_location_fields(batch, location_columns)
                        stager.write_batch(batch)

                run.rows_staged = stager.rows_staged
                run.rows_merged = stager.rows_merged
                run.rows_ingested = stager.rows_merged
                run.high_water_mark = now_utc

                if geojson_result and geojson_result.features_failed > 0:
                    run.metadata["parse_failures"] = geojson_result.features_failed
                    run.metadata["first_parse_error"] = geojson_result.failures[0]["error"]

                if geojson_result and geojson_result.unknown_properties:
                    run.metadata["unknown_properties"] = dict(geojson_result.unknown_properties)

            finally:
                filepath.unlink(missing_ok=True)

        return stager.rows_merged

    def _rename_file_columns(self, rows: list[dict], dataset_id: str) -> list[dict]:
        """Rename file export column names to API field names."""
        rename_map = self._get_metadata(dataset_id).column_rename_map
        if not rename_map:
            return rows
        return [{rename_map.get(k, k): v for k, v in row.items()} for row in rows]

    @retry(
        retry=retry_if_exception_type((ChunkedEncodingError, ConnectionError, ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, max=300),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _download_to_tempfile(self, url: str, download_format: str) -> Path:
        """Download a URL to a temporary file. Returns the file path."""
        suffix = ".geojson" if download_format == "GeoJSON" else ".csv"

        self.logger.info("Downloading %s", url)
        resp = self.client._session.get(url, stream=True, timeout=600)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="socrata_", delete=False)
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.close()
            filepath = Path(tmp.name)
            self.logger.info(
                "Downloaded %s to %s (%.1f MB)",
                url,
                filepath,
                filepath.stat().st_size / (1024 * 1024),
            )
            return filepath
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    @staticmethod
    def _convert_location_fields(
        rows: list[dict[str, Any]], location_columns: set[str]
    ) -> list[dict[str, Any]]:
        """Convert Socrata location/point JSON objects to EWKT strings."""
        for row in rows:
            for col in location_columns:
                val = row.get(col)
                if val is None:
                    continue
                if isinstance(val, str):
                    try:
                        val = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        row[col] = None
                        continue
                if isinstance(val, dict):
                    lat = val.get("latitude")
                    lon = val.get("longitude")
                    if lat is not None and lon is not None:
                        row[col] = f"SRID=4326;POINT({lon} {lat})"
                    else:
                        row[col] = None
                else:
                    row[col] = None
        return rows

    def _get_location_columns(self, dataset_id: str) -> set[str]:
        """Get columns that are Socrata location or point type."""
        meta = self._get_metadata(dataset_id)
        return {col.field_name for col in meta.columns if col.datatype in ("location", "point")}

    def full_refresh_via_api(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        entity_key: list[str] | None = None,
    ) -> int:
        """Full refresh using paginated SODA API (includes system fields)."""
        return self.incremental_update(
            dataset_id=dataset_id,
            target_table=target_table,
            target_schema=target_schema,
            config=IncrementalConfig(
                incremental_column=":updated_at",
                entity_key=entity_key or [],
            ),
            entity_key=entity_key,
            high_water_mark_override="",
        )

    def _rename_system_fields(self, rows: list[dict]) -> list[dict]:
        return [{self.SYSTEM_FIELD_RENAMES.get(k, k): v for k, v in row.items()} for row in rows]

    @staticmethod
    def _drop_computed_region_columns(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Remove Socrata computed region columns from rows."""
        for row in rows:
            keys_to_drop = [k for k in row if k.startswith(":@computed_region")]
            for k in keys_to_drop:
                del row[k]
        return rows

    def _preflight_column_check(
        self,
        source_columns: set[str],
        target_table: str,
        target_schema: str,
    ) -> None:
        """
        Compare source columns against the target table schema.
        Raises on new columns (schema drift). Logs warnings for
        columns present in the table but missing from the source.
        """
        table_columns = self._get_table_columns(target_table, target_schema)

        # Ignore metadata columns — they're added by our ingestion code, not the source
        data_columns_in_table = table_columns - self.METADATA_COLUMNS
        source_columns = source_columns - self.METADATA_COLUMNS

        new_in_source = source_columns - data_columns_in_table
        missing_from_source = data_columns_in_table - source_columns

        if missing_from_source:
            self.logger.warning(
                "Columns in %s.%s but not in source: %s",
                target_schema,
                target_table,
                missing_from_source,
            )

        if new_in_source:
            raise SchemaDriftError(
                f"Source has columns not in {target_schema}.{target_table}: "
                f"{new_in_source}. Add them via migration, then re-run."
            )

    def _get_hwm_from_table(
        self, target_table: str, target_schema: str
    ) -> tuple[str | None, str | None]:
        """
        Query the target table for the max socrata_updated_at and the
        max socrata_id at that timestamp. Returns (hwm_value, hwm_id).
        """
        fqn = f"{target_schema}.{target_table}"

        df = self.engine.query(
            f"""
            select to_char(socrata_updated_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') as hwm_value
            from {fqn}
            where socrata_updated_at is not null
            order by socrata_updated_at desc
            limit 1
            """,
        )
        if df.empty:
            return None, None

        hwm_value = df["hwm_value"].iloc[0]

        df_id = self.engine.query(
            f"""
            select socrata_id
            from {fqn}
            where to_char(socrata_updated_at at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US') = %(hwm_value)s
              and socrata_id is not null
            order by socrata_id desc
            limit 1
            """,
            {"hwm_value": hwm_value},
        )
        hwm_id = df_id["socrata_id"].iloc[0] if not df_id.empty else None

        return hwm_value, str(hwm_id) if hwm_id is not None else None

    def incremental_update(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        config: IncrementalConfig | None = None,
        entity_key: list[str] | None = None,
        high_water_mark_override: str | None = None,
    ) -> int:
        """
        Run an incremental paginated ingest using staged_ingest.

        All pages are accumulated in a staging table, then merged into the
        target in a single INSERT ... ON CONFLICT at the end. One
        IngestionTracker entry is created for the entire run.

        If entity_key is provided, uses SCD2 merge (hash-based versioning).
        Otherwise, uses simple INSERT with optional ON CONFLICT from config.
        """
        if high_water_mark_override is not None:
            raw_hwm = high_water_mark_override
            hwm_value, hwm_id = None, None
            if raw_hwm and "|" in raw_hwm:
                hwm_value, hwm_id = raw_hwm.rsplit("|", 1)
            elif raw_hwm:
                hwm_value = raw_hwm
        else:
            hwm_value, hwm_id = self._get_hwm_from_table(target_table, target_schema)

        if hwm_value:
            self.logger.info("Resuming from high_water_mark: %s (id: %s)", hwm_value, hwm_id)
        else:
            self.logger.info("No prior high_water_mark — full incremental load")

        meta = self._get_metadata(dataset_id)
        domain = meta.domain
        fqn = f"{target_schema}.{target_table}"
        inc_col = config.incremental_column

        run_metadata = {
            "mode": "incremental" if not high_water_mark_override == "" else "full_refresh_api",
            "incremental_column": inc_col,
            "entity_key": entity_key,
            "prior_high_water_mark": f"{hwm_value}|{hwm_id}" if hwm_id else hwm_value,
            "domain": domain,
        }

        # Determine merge strategy
        if entity_key:
            # SCD2 path
            staged_ingest_kwargs = {
                "entity_key": entity_key,
            }
        elif config.entity_key:
            # Simple conflict path
            staged_ingest_kwargs = {
                "conflict_column": config.entity_key,
                "conflict_action": "NOTHING",
            }
        else:
            staged_ingest_kwargs = {}

        with self.tracker.track(self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata) as run:
            where_parts = []
            if hwm_value and hwm_id:
                where_parts.append(
                    f"({inc_col} = '{hwm_value}' AND :id > '{hwm_id}') "
                    f"OR ({inc_col} > '{hwm_value}')"
                )
            elif hwm_value:
                where_parts.append(f"{inc_col} > '{hwm_value}'")
            if config.where:
                where_parts.append(config.where)

            combined_where = " AND ".join(f"({p})" for p in where_parts) if where_parts else None
            order_by = config.order_by or f"{inc_col}, :id"

            renamed_inc_col = self.SYSTEM_FIELD_RENAMES.get(inc_col, inc_col)
            max_hwm = hwm_value
            max_hwm_id = hwm_id
            location_columns = self._get_location_columns(dataset_id)

            with self.engine.staged_ingest(
                target_table=target_table,
                target_schema=target_schema,
                metadata_columns=self.METADATA_COLUMNS,
                **staged_ingest_kwargs,
            ) as stager:
                for page_num, batch in enumerate(
                    self.client.paginate(
                        domain=domain,
                        dataset_id=dataset_id,
                        columns=config.columns,
                        where=combined_where,
                        order_by=order_by,
                        include_system_fields=True,
                    ),
                    start=1,
                ):
                    if location_columns:
                        self._convert_location_fields(batch, location_columns)
                    renamed_batch = self._rename_system_fields(batch)
                    renamed_batch = self._drop_computed_region_columns(renamed_batch)
                    if page_num == 1:
                        source_columns = set(renamed_batch[0].keys())
                        self._preflight_column_check(source_columns, target_table, target_schema)
                    stager.write_batch(renamed_batch)

                    batch_hwm, batch_id = self._extract_max_hwm(renamed_batch, renamed_inc_col)
                    if batch_hwm and (
                        max_hwm is None
                        or batch_hwm > max_hwm
                        or (batch_hwm == max_hwm and batch_id > (max_hwm_id or ""))
                    ):
                        max_hwm = batch_hwm
                        max_hwm_id = batch_id

                    self.logger.info(
                        "Page %d: fetched %d (staged total: %d, hwm: %s|%s)",
                        page_num,
                        len(renamed_batch),
                        stager.rows_staged,
                        max_hwm,
                        max_hwm_id,
                    )

            # stager has now merged — record results on the run
            run.rows_staged = stager.rows_staged
            run.rows_merged = stager.rows_merged
            run.rows_ingested = stager.rows_merged
            run.high_water_mark = f"{max_hwm}|{max_hwm_id}" if max_hwm and max_hwm_id else max_hwm

        return stager.rows_merged

    @staticmethod
    def _extract_max_hwm(batch: list[dict[str, Any]], column: str) -> tuple[str | None, str | None]:
        """Return (max_column_value, id_at_max) from a batch."""
        values = [
            (row[column], str(row.get("socrata_id", "")))
            for row in batch
            if row.get(column) is not None
        ]
        if not values:
            return None, None
        best = max(values, key=lambda x: (x[0], x[1]))
        return str(best[0]), best[1]

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def preview(
        self,
        dataset_id: str,
        limit: int = 5,
        columns: list[str] | None = None,
        include_system_fields: bool = True,
    ) -> list[dict[str, Any]]:
        meta = self._get_metadata(dataset_id)
        return self.client.query(
            domain=meta.domain,
            dataset_id=dataset_id,
            select=", ".join(columns) if columns else None,
            limit=limit,
            include_system_fields=include_system_fields,
        )

    def print_ddl(
        self,
        dataset_id: str,
        schema: str = "raw_data",
        table_name: str | None = None,
    ) -> None:
        meta = self._get_metadata(dataset_id)
        print(meta.generate_ddl(schema=schema, table_name=table_name))
