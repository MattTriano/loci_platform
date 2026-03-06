"""
BikeIndexCollector — collects stolen bike reports from the Bike Index API
and ingests them into Postgres via StagedIngest.

Two-phase collection:

    1. collect_search(spec) — paginate the search API for summary-level
       data. Supports incremental mode: queries max(date_stolen) from
       the target table and skips rows at or before that timestamp.

    2. collect_detail(spec) — for bikes added/updated since the high
       water mark, fetch full detail via get_bike() and merge the
       enriched rows via StagedIngest (SCD2).

    3. collect(spec) — convenience method that runs both phases.

Usage:
    from loci.collectors.bike_index.client import BikeIndexClient
    from loci.collectors.bike_index.spec import BikeIndexDatasetSpec
    from loci.collectors.bike_index.collector import BikeIndexCollector

    spec = BikeIndexDatasetSpec(
        name="chicago_stolen_bikes",
        target_table="stolen_bikes",
        location="Chicago, IL",
        distance=10,
        entity_key=["id"],
    )

    client = BikeIndexClient(access_token="...")
    collector = BikeIndexCollector(client=client, engine=engine)

    # Full collection (both phases):
    summary = collector.collect(spec)

    # Or separately:
    summary = collector.collect_search(spec)
    summary = collector.collect_detail(spec)
"""

import json
import logging

from loci.collectors.bike_index.client import BikeIndexSearchParams
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

logger = logging.getLogger(__name__)


class BikeIndexCollector:
    """Collects stolen bike data from the Bike Index API and ingests
    into Postgres via StagedIngest.

    Parameters
    ----------
    client : BikeIndexClient
    engine : PostgresEngine
    tracker : IngestionTracker, optional
        If provided, logs each collection run.
    """

    SOURCE_NAME = "bike_index_api"

    EPOCH_HWM = 0  # Unix timestamp far enough in the past to capture everything

    METADATA_COLUMNS = {
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }

    def __init__(self, client, engine, tracker=None):
        self.client = client
        self.engine = engine
        self.tracker = tracker

    # ------------------------------------------------------------------
    # High water mark
    # ------------------------------------------------------------------

    def _get_high_water_mark(self, spec: BikeIndexDatasetSpec) -> int:
        """Query the target table for max(date_stolen) among current rows.

        Returns a unix timestamp, or EPOCH_HWM if the table is empty
        or doesn't exist yet.
        """
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select max(date_stolen) as hwm
                from {fqn}
                where "valid_to" is null
                """,
            )
            hwm = df["hwm"].iloc[0] if not df.empty else None
            return int(hwm) if hwm is not None else self.EPOCH_HWM
        except Exception:
            return self.EPOCH_HWM

    # ------------------------------------------------------------------
    # Flattening
    # ------------------------------------------------------------------

    def _flatten_search(self, summary: dict) -> dict:
        """Flatten a search result into a row dict (summary-level fields only).

        Detail-only columns are included as None so every row has a
        consistent set of keys.
        """
        coords = summary.get("stolen_coordinates") or []
        sc_lat = coords[0] if len(coords) == 2 else None
        sc_lon = coords[1] if len(coords) == 2 else None

        return {
            "id": summary.get("id"),
            "title": summary.get("title"),
            "serial": summary.get("serial"),
            "manufacturer_name": summary.get("manufacturer_name"),
            "frame_model": summary.get("frame_model"),
            "frame_colors": json.dumps(summary.get("frame_colors")),
            "year": summary.get("year"),
            "stolen": summary.get("stolen", True),
            "date_stolen": summary.get("date_stolen"),
            "description": summary.get("description"),
            "thumb": summary.get("thumb"),
            "url": summary.get("url"),
            "stolen_coordinates_lat": sc_lat,
            "stolen_coordinates_lon": sc_lon,
            "stolen_location": summary.get("stolen_location"),
            # Detail-only fields — null for search rows
            "latitude": None,
            "longitude": None,
            "theft_description": None,
            "locking_description": None,
            "lock_defeat_description": None,
            "police_report_number": None,
            "police_report_department": None,
            "propulsion_type_slug": summary.get("propulsion_type_slug"),
            "cycle_type_slug": summary.get("cycle_type_slug"),
            "status": summary.get("status"),
            "registration_created_at": None,
            "registration_updated_at": None,
            "manufacturer_id": None,
            "paint_description": None,
            "frame_size": None,
            "frame_material_slug": None,
            "handlebar_type_slug": None,
            "front_gear_type_slug": None,
            "rear_gear_type_slug": None,
            "rear_wheel_size_iso_bsd": None,
            "front_wheel_size_iso_bsd": None,
            "rear_tire_narrow": None,
            "front_tire_narrow": None,
            "extra_registration_number": None,
            "additional_registration": None,
            "components": None,
            "public_images": None,
        }

    def _flatten_detail(self, detail: dict) -> dict:
        """Flatten a get_bike() response into a full row dict."""
        stolen = detail.get("stolen_record") or {}

        coords = detail.get("stolen_coordinates") or []
        sc_lat = coords[0] if len(coords) == 2 else None
        sc_lon = coords[1] if len(coords) == 2 else None

        return {
            "id": detail.get("id"),
            "title": detail.get("title"),
            "serial": detail.get("serial"),
            "manufacturer_name": detail.get("manufacturer_name"),
            "frame_model": detail.get("frame_model"),
            "frame_colors": json.dumps(detail.get("frame_colors")),
            "year": detail.get("year"),
            "stolen": detail.get("stolen", True),
            "date_stolen": detail.get("date_stolen"),
            "description": detail.get("description"),
            "thumb": detail.get("thumb"),
            "url": detail.get("url"),
            "stolen_coordinates_lat": sc_lat,
            "stolen_coordinates_lon": sc_lon,
            "stolen_location": detail.get("stolen_location"),
            # From stolen_record
            "latitude": stolen.get("latitude"),
            "longitude": stolen.get("longitude"),
            "theft_description": stolen.get("theft_description"),
            "locking_description": stolen.get("locking_description"),
            "lock_defeat_description": stolen.get("lock_defeat_description"),
            "police_report_number": stolen.get("police_report_number"),
            "police_report_department": stolen.get("police_report_department"),
            # Detail-only scalar fields
            "propulsion_type_slug": detail.get("propulsion_type_slug"),
            "cycle_type_slug": detail.get("cycle_type_slug"),
            "status": detail.get("status"),
            "registration_created_at": detail.get("registration_created_at"),
            "registration_updated_at": detail.get("registration_updated_at"),
            "manufacturer_id": detail.get("manufacturer_id"),
            "paint_description": detail.get("paint_description"),
            "frame_size": detail.get("frame_size"),
            "frame_material_slug": detail.get("frame_material_slug"),
            "handlebar_type_slug": detail.get("handlebar_type_slug"),
            "front_gear_type_slug": detail.get("front_gear_type_slug"),
            "rear_gear_type_slug": detail.get("rear_gear_type_slug"),
            "rear_wheel_size_iso_bsd": detail.get("rear_wheel_size_iso_bsd"),
            "front_wheel_size_iso_bsd": detail.get("front_wheel_size_iso_bsd"),
            "rear_tire_narrow": detail.get("rear_tire_narrow"),
            "front_tire_narrow": detail.get("front_tire_narrow"),
            "extra_registration_number": detail.get("extra_registration_number"),
            "additional_registration": detail.get("additional_registration"),
            # Nested arrays as jsonb
            "components": json.dumps(detail.get("components")),
            "public_images": json.dumps(detail.get("public_images")),
        }

    # ------------------------------------------------------------------
    # Collection phases
    # ------------------------------------------------------------------

    def collect(
        self,
        spec: BikeIndexDatasetSpec,
        force: bool = False,
        batch_size: int = 50,
    ) -> dict:
        """Run both collection phases: search then detail.

        Parameters
        ----------
        spec : BikeIndexDatasetSpec
        force : bool
            If True, full refresh (ignore high water mark).
        batch_size : int
            Records per write_batch call.

        Returns
        -------
        dict with keys: spec_name, search, detail.
        """
        hwm = self.EPOCH_HWM if force else self._get_high_water_mark(spec)
        search_summary = self.collect_search(
            spec, force=force, batch_size=batch_size, high_water_mark=hwm
        )
        detail_summary = self.collect_detail(
            spec, force=force, batch_size=batch_size, high_water_mark=hwm
        )
        return {
            "spec_name": spec.name,
            "search": search_summary,
            "detail": detail_summary,
        }

    def collect_search(
        self,
        spec: BikeIndexDatasetSpec,
        force: bool = False,
        batch_size: int = 50,
        high_water_mark: int | None = None,
    ) -> dict:
        """Phase 1: paginate the search API and ingest summary-level rows.

        In incremental mode (force=False), rows with date_stolen <= the
        high water mark are skipped.

        Parameters
        ----------
        spec : BikeIndexDatasetSpec
        force : bool
            If True, ingest all results regardless of date_stolen.
        batch_size : int
            Records per write_batch call.
        high_water_mark : int | None
            If provided, use this HWM instead of querying the target table.

        Returns
        -------
        dict with counts of rows staged/merged and any errors.
        """
        if high_water_mark is not None:
            hwm = high_water_mark
        elif force:
            hwm = self.EPOCH_HWM
        else:
            hwm = self._get_high_water_mark(spec)
        logger.info(
            "collect_search %r: high_water_mark=%d (force=%s)",
            spec.name,
            hwm,
            force,
        )

        summary = {
            "spec_name": spec.name,
            "phase": "search",
            "high_water_mark": hwm,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "rows_skipped": 0,
            "errors": [],
        }

        search = BikeIndexSearchParams(**spec.to_search_params())
        dataset_id = f"{spec.name}/{spec.location}/search"

        def _run():
            return self._ingest_search(spec, search, hwm, batch_size)

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table}",
                metadata={
                    "phase": "search",
                    "location": spec.location,
                    "distance": spec.distance,
                    "high_water_mark": hwm,
                    "force": force,
                },
            ) as run:
                staged, merged, skipped = _run()
                run.rows_staged = staged
                run.rows_merged = merged
        else:
            staged, merged, skipped = _run()

        summary["total_rows_staged"] = staged
        summary["total_rows_merged"] = merged
        summary["rows_skipped"] = skipped

        logger.info("collect_search complete for %r: %s", spec.name, summary)
        return summary

    def _ingest_search(
        self,
        spec: BikeIndexDatasetSpec,
        search: BikeIndexSearchParams,
        hwm: int,
        batch_size: int,
    ) -> tuple[int, int, int]:
        """Paginate search results, skip rows at or before hwm, ingest the rest.

        Returns (rows_staged, rows_merged, rows_skipped).
        """
        skipped = 0

        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=self.METADATA_COLUMNS,
        ) as stager:
            batch: list[dict] = []
            total = 0

            for page_bikes in self.client.search_all(search):
                for bike in page_bikes:
                    date_stolen = bike.get("date_stolen")
                    if date_stolen is not None and date_stolen <= hwm:
                        skipped += 1
                        continue

                    row = self._flatten_search(bike)
                    batch.append(row)
                    total += 1

                    if len(batch) >= batch_size:
                        logger.info("Writing search batch of %d (total: %d)", len(batch), total)
                        stager.write_batch(batch)
                        batch = []

            if batch:
                logger.info("Writing final search batch of %d (total: %d)", len(batch), total)
                stager.write_batch(batch)

        logger.info(
            "Search ingest %s: staged=%d merged=%d skipped=%d",
            spec.name,
            stager.rows_staged,
            stager.rows_merged,
            skipped,
        )
        return stager.rows_staged, stager.rows_merged, skipped

    def collect_detail(
        self,
        spec: BikeIndexDatasetSpec,
        force: bool = False,
        batch_size: int = 50,
        high_water_mark: int | None = None,
    ) -> dict:
        """Phase 2: fetch full detail for bikes past the high water mark.

        Queries the target table for ids where date_stolen > hwm,
        ordered by (date_stolen, id) for idempotent resumability.
        For each, calls get_bike() and merges the enriched row via
        StagedIngest.

        Parameters
        ----------
        spec : BikeIndexDatasetSpec
        force : bool
            If True, fetch detail for all current rows.
        batch_size : int
            Records per write_batch call.
        high_water_mark : int | None
            If provided, use this HWM instead of querying the target table.

        Returns
        -------
        dict with counts of rows staged/merged and any errors.
        """
        if high_water_mark is not None:
            hwm = high_water_mark
        elif force:
            hwm = self.EPOCH_HWM
        else:
            hwm = self._get_high_water_mark(spec)
        logger.info(
            "collect_detail %r: high_water_mark=%d (force=%s)",
            spec.name,
            hwm,
            force,
        )

        summary = {
            "spec_name": spec.name,
            "phase": "detail",
            "high_water_mark": hwm,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "errors": [],
        }

        bike_ids = self._get_ids_needing_detail(spec, hwm)
        logger.info("collect_detail: %d bikes to fetch", len(bike_ids))

        if not bike_ids:
            return summary

        dataset_id = f"{spec.name}/{spec.location}/detail"

        def _run():
            return self._ingest_detail(spec, bike_ids, batch_size)

        if self.tracker:
            with self.tracker.track(
                source=self.SOURCE_NAME,
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table}",
                metadata={
                    "phase": "detail",
                    "high_water_mark": hwm,
                    "force": force,
                    "num_bikes": len(bike_ids),
                },
            ) as run:
                staged, merged, errors = _run()
                run.rows_staged = staged
                run.rows_merged = merged
        else:
            staged, merged, errors = _run()

        summary["total_rows_staged"] = staged
        summary["total_rows_merged"] = merged
        summary["errors"] = errors

        logger.info("collect_detail complete for %r: %s", spec.name, summary)
        return summary

    def _get_ids_needing_detail(
        self,
        spec: BikeIndexDatasetSpec,
        hwm: int,
    ) -> list[int]:
        """Query target table for bike ids with date_stolen > hwm, ordered
        by (date_stolen, id) for deterministic, resumable iteration."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(
                f"""
                select id
                from {fqn}
                where "valid_to" is null
                  and date_stolen > %(hwm)s
                order by date_stolen, id
                """,
                {"hwm": hwm},
            )
            return df["id"].tolist()
        except Exception:
            logger.warning("Could not query ids from %s", fqn, exc_info=True)
            return []

    def _ingest_detail(
        self,
        spec: BikeIndexDatasetSpec,
        bike_ids: list[int],
        batch_size: int,
    ) -> tuple[int, int, list[dict]]:
        """Fetch detail for each bike id and ingest via StagedIngest.

        Returns (rows_staged, rows_merged, errors).
        """
        errors = []

        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=self.METADATA_COLUMNS,
        ) as stager:
            batch: list[dict] = []
            total = 0

            for bike_id in bike_ids:
                try:
                    detail = self.client.get_bike(bike_id)
                except Exception:
                    logger.warning("Failed to fetch detail for bike %s", bike_id, exc_info=True)
                    errors.append({"bike_id": bike_id, "error": "fetch_failed"})
                    continue

                row = self._flatten_detail(detail)
                batch.append(row)
                total += 1

                if len(batch) >= batch_size:
                    logger.info("Writing detail batch of %d (total: %d)", len(batch), total)
                    stager.write_batch(batch)
                    batch = []

            if batch:
                logger.info("Writing final detail batch of %d (total: %d)", len(batch), total)
                stager.write_batch(batch)

        logger.info(
            "Detail ingest %s: staged=%d merged=%d errors=%d",
            spec.name,
            stager.rows_staged,
            stager.rows_merged,
            len(errors),
        )
        return stager.rows_staged, stager.rows_merged, errors

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def generate_ddl(self, spec: BikeIndexDatasetSpec) -> str:
        """Generate a CREATE TABLE statement for the bike index target table."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        entity_key = spec.entity_key or ["id"]
        ek_cols = ", ".join(f'"{c}"' for c in entity_key)

        columns = [
            # Core fields (from search + detail)
            '"id" integer not null',
            '"title" text',
            '"serial" text',
            '"manufacturer_name" text',
            '"frame_model" text',
            '"frame_colors" jsonb',
            '"year" integer',
            '"stolen" boolean',
            '"date_stolen" bigint',
            '"description" text',
            '"thumb" text',
            '"url" text',
            # Coordinates from search summary
            '"stolen_coordinates_lat" double precision',
            '"stolen_coordinates_lon" double precision',
            '"stolen_location" text',
            # Coordinates from detail stolen_record
            '"latitude" double precision',
            '"longitude" double precision',
            # stolen_record fields
            '"theft_description" text',
            '"locking_description" text',
            '"lock_defeat_description" text',
            '"police_report_number" text',
            '"police_report_department" text',
            # Detail-only scalar fields
            '"propulsion_type_slug" text',
            '"cycle_type_slug" text',
            '"status" text',
            '"registration_created_at" bigint',
            '"registration_updated_at" bigint',
            '"manufacturer_id" integer',
            '"paint_description" text',
            '"frame_size" text',
            '"frame_material_slug" text',
            '"handlebar_type_slug" text',
            '"front_gear_type_slug" text',
            '"rear_gear_type_slug" text',
            '"rear_wheel_size_iso_bsd" integer',
            '"front_wheel_size_iso_bsd" integer',
            '"rear_tire_narrow" boolean',
            '"front_tire_narrow" boolean',
            '"extra_registration_number" text',
            '"additional_registration" text',
            # Nested arrays as jsonb
            '"components" jsonb',
            '"public_images" jsonb',
            # Metadata / SCD2
            "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
            '"valid_to" timestamptz',
        ]

        lines = [f"create table {fqn} ("]
        lines.append("    " + ",\n    ".join(columns))
        lines.append(");")

        # Unique constraint on entity key + record_hash
        constraint_name = f"uq_{spec.target_table}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        # Partial index for current versions
        index_name = f"ix_{spec.target_table}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

        return "\n".join(lines)


# """
# BikeIndexCollector — collects stolen bike reports from the Bike Index API
# and ingests them into Postgres via StagedIngest.

# Two-phase collection:

#     1. collect_search(spec) — paginate the search API for summary-level
#        data. Supports incremental mode: queries max(date_stolen) from
#        the target table and skips rows at or before that timestamp.

#     2. collect_detail(spec) — for bikes added/updated since the high
#        water mark, fetch full detail via get_bike() and merge the
#        enriched rows via StagedIngest (SCD2).

#     3. collect(spec) — convenience method that runs both phases.

# Usage:
#     from loci.collectors.bike_index.client import BikeIndexClient
#     from loci.collectors.bike_index.spec import BikeIndexDatasetSpec
#     from loci.collectors.bike_index.collector import BikeIndexCollector

#     spec = BikeIndexDatasetSpec(
#         name="chicago_stolen_bikes",
#         target_table="stolen_bikes",
#         location="Chicago, IL",
#         distance=10,
#         entity_key=["id"],
#     )

#     client = BikeIndexClient(access_token="...")
#     collector = BikeIndexCollector(client=client, engine=engine)

#     # Full collection (both phases):
#     summary = collector.collect(spec)

#     # Or separately:
#     summary = collector.collect_search(spec)
#     summary = collector.collect_detail(spec)
# """

# import json
# import logging

# from loci.collectors.bike_index.client import BikeIndexSearchParams
# from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

# logger = logging.getLogger(__name__)


# class BikeIndexCollector:
#     """Collects stolen bike data from the Bike Index API and ingests
#     into Postgres via StagedIngest.

#     Parameters
#     ----------
#     client : BikeIndexClient
#     engine : PostgresEngine
#     tracker : IngestionTracker, optional
#         If provided, logs each collection run.
#     """

#     SOURCE_NAME = "bike_index_api"
#     EPOCH_HWM = 0   # Unix timestamp far enough in the past to capture everything
#     METADATA_COLUMNS = {
#         "ingested_at",
#         "record_hash",
#         "valid_from",
#         "valid_to",
#     }

#     def __init__(self, client, engine, tracker=None):
#         self.client = client
#         self.engine = engine
#         self.tracker = tracker

#     # ------------------------------------------------------------------
#     # High water mark
#     # ------------------------------------------------------------------

#     def _get_high_water_mark(self, spec: BikeIndexDatasetSpec) -> int:
#         """Query the target table for max(date_stolen) among current rows.

#         Returns a unix timestamp, or EPOCH_HWM if the table is empty
#         or doesn't exist yet.
#         """
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         try:
#             df = self.engine.query(
#                 f"""
#                 select max(date_stolen) as hwm
#                 from {fqn}
#                 where "valid_to" is null
#                 """,
#             )
#             hwm = df["hwm"].iloc[0] if not df.empty else None
#             return int(hwm) if hwm is not None else self.EPOCH_HWM
#         except Exception:
#             return self.EPOCH_HWM

#     # ------------------------------------------------------------------
#     # Flattening
#     # ------------------------------------------------------------------

#     def _flatten_search(self, summary: dict) -> dict:
#         """Flatten a search result into a row dict (summary-level fields only).

#         Detail-only columns are included as None so every row has a
#         consistent set of keys.
#         """
#         coords = summary.get("stolen_coordinates") or []
#         sc_lat = coords[0] if len(coords) == 2 else None
#         sc_lon = coords[1] if len(coords) == 2 else None

#         return {
#             "id": summary.get("id"),
#             "title": summary.get("title"),
#             "serial": summary.get("serial"),
#             "manufacturer_name": summary.get("manufacturer_name"),
#             "frame_model": summary.get("frame_model"),
#             "frame_colors": json.dumps(summary.get("frame_colors")),
#             "year": summary.get("year"),
#             "stolen": summary.get("stolen", True),
#             "date_stolen": summary.get("date_stolen"),
#             "description": summary.get("description"),
#             "thumb": summary.get("thumb"),
#             "url": summary.get("url"),
#             "stolen_coordinates_lat": sc_lat,
#             "stolen_coordinates_lon": sc_lon,
#             "stolen_location": summary.get("stolen_location"),
#             # Detail-only fields — null for search rows
#             "latitude": None,
#             "longitude": None,
#             "theft_description": None,
#             "locking_description": None,
#             "lock_defeat_description": None,
#             "police_report_number": None,
#             "police_report_department": None,
#             "propulsion_type_slug": summary.get("propulsion_type_slug"),
#             "cycle_type_slug": summary.get("cycle_type_slug"),
#             "status": summary.get("status"),
#             "registration_created_at": None,
#             "registration_updated_at": None,
#             "manufacturer_id": None,
#             "paint_description": None,
#             "frame_size": None,
#             "frame_material_slug": None,
#             "handlebar_type_slug": None,
#             "front_gear_type_slug": None,
#             "rear_gear_type_slug": None,
#             "rear_wheel_size_iso_bsd": None,
#             "front_wheel_size_iso_bsd": None,
#             "rear_tire_narrow": None,
#             "front_tire_narrow": None,
#             "extra_registration_number": None,
#             "additional_registration": None,
#             "components": None,
#             "public_images": None,
#         }

#     def _flatten_detail(self, detail: dict) -> dict:
#         """Flatten a get_bike() response into a full row dict."""
#         stolen = detail.get("stolen_record") or {}

#         coords = detail.get("stolen_coordinates") or []
#         sc_lat = coords[0] if len(coords) == 2 else None
#         sc_lon = coords[1] if len(coords) == 2 else None

#         return {
#             "id": detail.get("id"),
#             "title": detail.get("title"),
#             "serial": detail.get("serial"),
#             "manufacturer_name": detail.get("manufacturer_name"),
#             "frame_model": detail.get("frame_model"),
#             "frame_colors": json.dumps(detail.get("frame_colors")),
#             "year": detail.get("year"),
#             "stolen": detail.get("stolen", True),
#             "date_stolen": detail.get("date_stolen"),
#             "description": detail.get("description"),
#             "thumb": detail.get("thumb"),
#             "url": detail.get("url"),
#             "stolen_coordinates_lat": sc_lat,
#             "stolen_coordinates_lon": sc_lon,
#             "stolen_location": detail.get("stolen_location"),
#             # From stolen_record
#             "latitude": stolen.get("latitude"),
#             "longitude": stolen.get("longitude"),
#             "theft_description": stolen.get("theft_description"),
#             "locking_description": stolen.get("locking_description"),
#             "lock_defeat_description": stolen.get("lock_defeat_description"),
#             "police_report_number": stolen.get("police_report_number"),
#             "police_report_department": stolen.get("police_report_department"),
#             # Detail-only scalar fields
#             "propulsion_type_slug": detail.get("propulsion_type_slug"),
#             "cycle_type_slug": detail.get("cycle_type_slug"),
#             "status": detail.get("status"),
#             "registration_created_at": detail.get("registration_created_at"),
#             "registration_updated_at": detail.get("registration_updated_at"),
#             "manufacturer_id": detail.get("manufacturer_id"),
#             "paint_description": detail.get("paint_description"),
#             "frame_size": detail.get("frame_size"),
#             "frame_material_slug": detail.get("frame_material_slug"),
#             "handlebar_type_slug": detail.get("handlebar_type_slug"),
#             "front_gear_type_slug": detail.get("front_gear_type_slug"),
#             "rear_gear_type_slug": detail.get("rear_gear_type_slug"),
#             "rear_wheel_size_iso_bsd": detail.get("rear_wheel_size_iso_bsd"),
#             "front_wheel_size_iso_bsd": detail.get("front_wheel_size_iso_bsd"),
#             "rear_tire_narrow": detail.get("rear_tire_narrow"),
#             "front_tire_narrow": detail.get("front_tire_narrow"),
#             "extra_registration_number": detail.get("extra_registration_number"),
#             "additional_registration": detail.get("additional_registration"),
#             # Nested arrays as jsonb
#             "components": json.dumps(detail.get("components")),
#             "public_images": json.dumps(detail.get("public_images")),
#         }

#     # ------------------------------------------------------------------
#     # Collection phases
#     # ------------------------------------------------------------------

#     def collect(
#         self,
#         spec: BikeIndexDatasetSpec,
#         force: bool = False,
#         batch_size: int = 50,
#     ) -> dict:
#         """Run both collection phases: search then detail.

#         Parameters
#         ----------
#         spec : BikeIndexDatasetSpec
#         force : bool
#             If True, full refresh (ignore high water mark).
#         batch_size : int
#             Records per write_batch call.

#         Returns
#         -------
#         dict with keys: spec_name, search, detail.
#         """
#         search_summary = self.collect_search(spec, force=force, batch_size=batch_size)
#         detail_summary = self.collect_detail(spec, force=force, batch_size=batch_size)
#         return {
#             "spec_name": spec.name,
#             "search": search_summary,
#             "detail": detail_summary,
#         }

#     def collect_search(
#         self,
#         spec: BikeIndexDatasetSpec,
#         force: bool = False,
#         batch_size: int = 50,
#     ) -> dict:
#         """Phase 1: paginate the search API and ingest summary-level rows.

#         In incremental mode (force=False), rows with date_stolen <= the
#         high water mark are skipped.

#         Parameters
#         ----------
#         spec : BikeIndexDatasetSpec
#         force : bool
#             If True, ingest all results regardless of date_stolen.
#         batch_size : int
#             Records per write_batch call.

#         Returns
#         -------
#         dict with counts of rows staged/merged and any errors.
#         """
#         hwm = self.EPOCH_HWM if force else self._get_high_water_mark(spec)
#         logger.info(
#             "collect_search %r: high_water_mark=%d (force=%s)",
#             spec.name, hwm, force,
#         )

#         summary = {
#             "spec_name": spec.name,
#             "phase": "search",
#             "high_water_mark": hwm,
#             "total_rows_staged": 0,
#             "total_rows_merged": 0,
#             "rows_skipped": 0,
#             "errors": [],
#         }

#         search = BikeIndexSearchParams(**spec.to_search_params())
#         dataset_id = f"{spec.name}/{spec.location}/search"

#         def _run():
#             return self._ingest_search(spec, search, hwm, batch_size)

#         if self.tracker:
#             with self.tracker.track(
#                 source=self.SOURCE_NAME,
#                 dataset_id=dataset_id,
#                 target_table=f"{spec.target_schema}.{spec.target_table}",
#                 metadata={
#                     "phase": "search",
#                     "location": spec.location,
#                     "distance": spec.distance,
#                     "high_water_mark": hwm,
#                     "force": force,
#                 },
#             ) as run:
#                 staged, merged, skipped = _run()
#                 run.rows_staged = staged
#                 run.rows_merged = merged
#         else:
#             staged, merged, skipped = _run()

#         summary["total_rows_staged"] = staged
#         summary["total_rows_merged"] = merged
#         summary["rows_skipped"] = skipped

#         logger.info("collect_search complete for %r: %s", spec.name, summary)
#         return summary

#     def _ingest_search(
#         self,
#         spec: BikeIndexDatasetSpec,
#         search: BikeIndexSearchParams,
#         hwm: int,
#         batch_size: int,
#     ) -> tuple[int, int, int]:
#         """Paginate search results, skip rows at or before hwm, ingest the rest.

#         Returns (rows_staged, rows_merged, rows_skipped).
#         """
#         skipped = 0

#         with self.engine.staged_ingest(
#             target_table=spec.target_table,
#             target_schema=spec.target_schema,
#             entity_key=spec.entity_key,
#             metadata_columns=self.METADATA_COLUMNS,
#         ) as stager:
#             batch: list[dict] = []
#             total = 0

#             for page_bikes in self.client.search_all(search):
#                 for bike in page_bikes:
#                     date_stolen = bike.get("date_stolen")
#                     if date_stolen is not None and date_stolen <= hwm:
#                         skipped += 1
#                         continue

#                     row = self._flatten_search(bike)
#                     batch.append(row)
#                     total += 1

#                     if len(batch) >= batch_size:
#                         logger.info("Writing search batch of %d (total: %d)", len(batch), total)
#                         stager.write_batch(batch)
#                         batch = []

#             if batch:
#                 logger.info("Writing final search batch of %d (total: %d)", len(batch), total)
#                 stager.write_batch(batch)

#         logger.info(
#             "Search ingest %s: staged=%d merged=%d skipped=%d",
#             spec.name, stager.rows_staged, stager.rows_merged, skipped,
#         )
#         return stager.rows_staged, stager.rows_merged, skipped

#     def collect_detail(
#         self,
#         spec: BikeIndexDatasetSpec,
#         force: bool = False,
#         batch_size: int = 50,
#     ) -> dict:
#         """Phase 2: fetch full detail for bikes past the high water mark.

#         Queries the target table for ids where date_stolen > hwm,
#         ordered by (date_stolen, id) for idempotent resumability.
#         For each, calls get_bike() and merges the enriched row via
#         StagedIngest.

#         Parameters
#         ----------
#         spec : BikeIndexDatasetSpec
#         force : bool
#             If True, fetch detail for all current rows.
#         batch_size : int
#             Records per write_batch call.

#         Returns
#         -------
#         dict with counts of rows staged/merged and any errors.
#         """
#         hwm = self.EPOCH_HWM if force else self._get_high_water_mark(spec)
#         logger.info(
#             "collect_detail %r: high_water_mark=%d (force=%s)",
#             spec.name, hwm, force,
#         )

#         summary = {
#             "spec_name": spec.name,
#             "phase": "detail",
#             "high_water_mark": hwm,
#             "total_rows_staged": 0,
#             "total_rows_merged": 0,
#             "errors": [],
#         }

#         bike_ids = self._get_ids_needing_detail(spec, hwm)
#         logger.info("collect_detail: %d bikes to fetch", len(bike_ids))

#         if not bike_ids:
#             return summary

#         dataset_id = f"{spec.name}/{spec.location}/detail"

#         def _run():
#             return self._ingest_detail(spec, bike_ids, batch_size)

#         if self.tracker:
#             with self.tracker.track(
#                 source=self.SOURCE_NAME,
#                 dataset_id=dataset_id,
#                 target_table=f"{spec.target_schema}.{spec.target_table}",
#                 metadata={
#                     "phase": "detail",
#                     "high_water_mark": hwm,
#                     "force": force,
#                     "num_bikes": len(bike_ids),
#                 },
#             ) as run:
#                 staged, merged, errors = _run()
#                 run.rows_staged = staged
#                 run.rows_merged = merged
#         else:
#             staged, merged, errors = _run()

#         summary["total_rows_staged"] = staged
#         summary["total_rows_merged"] = merged
#         summary["errors"] = errors

#         logger.info("collect_detail complete for %r: %s", spec.name, summary)
#         return summary

#     def _get_ids_needing_detail(
#         self,
#         spec: BikeIndexDatasetSpec,
#         hwm: int,
#     ) -> list[int]:
#         """Query target table for bike ids with date_stolen > hwm, ordered
#         by (date_stolen, id) for deterministic, resumable iteration."""
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         try:
#             df = self.engine.query(
#                 f"""
#                 select id
#                 from {fqn}
#                 where "valid_to" is null
#                   and date_stolen > %(hwm)s
#                 order by date_stolen, id
#                 """,
#                 {"hwm": hwm},
#             )
#             return df["id"].tolist()
#         except Exception:
#             logger.warning("Could not query ids from %s", fqn, exc_info=True)
#             return []

#     def _ingest_detail(
#         self,
#         spec: BikeIndexDatasetSpec,
#         bike_ids: list[int],
#         batch_size: int,
#     ) -> tuple[int, int, list[dict]]:
#         """Fetch detail for each bike id and ingest via StagedIngest.

#         Returns (rows_staged, rows_merged, errors).
#         """
#         errors = []

#         with self.engine.staged_ingest(
#             target_table=spec.target_table,
#             target_schema=spec.target_schema,
#             entity_key=spec.entity_key,
#             metadata_columns=self.METADATA_COLUMNS,
#         ) as stager:
#             batch: list[dict] = []
#             total = 0

#             for bike_id in bike_ids:
#                 try:
#                     detail = self.client.get_bike(bike_id)
#                 except Exception:
#                     logger.warning("Failed to fetch detail for bike %s", bike_id, exc_info=True)
#                     errors.append({"bike_id": bike_id, "error": "fetch_failed"})
#                     continue

#                 row = self._flatten_detail(detail)
#                 batch.append(row)
#                 total += 1

#                 if len(batch) >= batch_size:
#                     logger.info("Writing detail batch of %d (total: %d)", len(batch), total)
#                     stager.write_batch(batch)
#                     batch = []

#             if batch:
#                 logger.info("Writing final detail batch of %d (total: %d)", len(batch), total)
#                 stager.write_batch(batch)

#         logger.info(
#             "Detail ingest %s: staged=%d merged=%d errors=%d",
#             spec.name, stager.rows_staged, stager.rows_merged, len(errors),
#         )
#         return stager.rows_staged, stager.rows_merged, errors

#     # ------------------------------------------------------------------
#     # DDL
#     # ------------------------------------------------------------------

#     def generate_ddl(self, spec: BikeIndexDatasetSpec) -> str:
#         """Generate a CREATE TABLE statement for the bike index target table."""
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         entity_key = spec.entity_key or ["id"]
#         ek_cols = ", ".join(f'"{c}"' for c in entity_key)

#         columns = [
#             # Core fields (from search + detail)
#             '"id" integer not null',
#             '"title" text',
#             '"serial" text',
#             '"manufacturer_name" text',
#             '"frame_model" text',
#             '"frame_colors" jsonb',
#             '"year" integer',
#             '"stolen" boolean',
#             '"date_stolen" bigint',
#             '"description" text',
#             '"thumb" text',
#             '"url" text',
#             # Coordinates from search summary
#             '"stolen_coordinates_lat" double precision',
#             '"stolen_coordinates_lon" double precision',
#             '"stolen_location" text',
#             # Coordinates from detail stolen_record
#             '"latitude" double precision',
#             '"longitude" double precision',
#             # stolen_record fields
#             '"theft_description" text',
#             '"locking_description" text',
#             '"lock_defeat_description" text',
#             '"police_report_number" text',
#             '"police_report_department" text',
#             # Detail-only scalar fields
#             '"propulsion_type_slug" text',
#             '"cycle_type_slug" text',
#             '"status" text',
#             '"registration_created_at" bigint',
#             '"registration_updated_at" bigint',
#             '"manufacturer_id" integer',
#             '"paint_description" text',
#             '"frame_size" text',
#             '"frame_material_slug" text',
#             '"handlebar_type_slug" text',
#             '"front_gear_type_slug" text',
#             '"rear_gear_type_slug" text',
#             '"rear_wheel_size_iso_bsd" integer',
#             '"front_wheel_size_iso_bsd" integer',
#             '"rear_tire_narrow" boolean',
#             '"front_tire_narrow" boolean',
#             '"extra_registration_number" text',
#             '"additional_registration" text',
#             # Nested arrays as jsonb
#             '"components" jsonb',
#             '"public_images" jsonb',
#             # Metadata / SCD2
#             "\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')",
#             '"record_hash" text not null',
#             "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
#             '"valid_to" timestamptz',
#         ]

#         lines = [f"create table {fqn} ("]
#         lines.append("    " + ",\n    ".join(columns))
#         lines.append(");")

#         # Unique constraint on entity key + record_hash
#         constraint_name = f"uq_{spec.target_table}_entity_hash"
#         lines.append("")
#         lines.append(f"alter table {fqn}")
#         lines.append(f"    add constraint {constraint_name}")
#         lines.append(f'    unique ({ek_cols}, "record_hash");')

#         # Partial index for current versions
#         index_name = f"ix_{spec.target_table}_current"
#         lines.append("")
#         lines.append(f"create index {index_name}")
#         lines.append(f"    on {fqn} ({ek_cols})")
#         lines.append('    where "valid_to" is null;')

#         return "\n".join(lines)
