"""
BikeIndexCollector — collects stolen bike reports from the Bike Index API
and ingests them into Postgres via StagedIngest.

The Bike Index search endpoint returns summary data per bike. To get
latitude/longitude for each theft, we need a second call to /bikes/{id}
for each bike (the stolen_record with coordinates is only on the detail
endpoint). This is the main bottleneck — one HTTP call per bike.

Strategy:
    1. Use search_all() to paginate through stolen bikes near a location.
    2. For each bike in the search results, call get_bike() to get the
       stolen_record with lat/lon (controlled by spec.fetch_detail).
    3. Flatten into a dict matching the target schema.
    4. Write batches via StagedIngest.

Usage:
    from bike_index.client import BikeIndexClient
    from bike_index.spec import BikeIndexDatasetSpec
    from bike_index.collector import BikeIndexCollector

    spec = BikeIndexDatasetSpec(
        name="chicago_stolen_bikes",
        target_table="stolen_bikes",
        location="Chicago, IL",
        distance=10,
        entity_key=["id"],
    )

    client = BikeIndexClient(access_token="...")
    collector = BikeIndexCollector(client=client, engine=engine)
    summary = collector.collect(spec)
"""

import json
import logging

from loci.collectors.bike_index.client import BikeIndexSearchParams
from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

logger = logging.getLogger(__name__)

METADATA_COLUMNS = {
    "ingested_at",
    "record_hash",
    "valid_from",
    "valid_to",
}


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

    def __init__(self, client, engine, tracker=None):
        self.client = client
        self.engine = engine
        self.tracker = tracker

    def _flatten_bike(self, summary: dict, detail: dict | None) -> dict:
        """Flatten a bike summary + detail response into a single row dict.

        Returns a dict with a stable set of keys suitable for insertion
        into a Postgres table. Coordinates come from the detail endpoint's
        stolen_record when available, falling back to stolen_coordinates
        from the search summary.

        Lists (like frame_colors) are serialized to JSON strings for
        jsonb columns.
        """
        stolen = (detail or {}).get("stolen_record") or {}

        # Prefer detail lat/lon, fall back to summary stolen_coordinates
        lat = stolen.get("latitude")
        lon = stolen.get("longitude")
        if lat is None or lon is None:
            coords = summary.get("stolen_coordinates") or []
            if len(coords) == 2:
                lat, lon = coords[0], coords[1]

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
            "latitude": lat,
            "longitude": lon,
            "stolen_location": summary.get("stolen_location"),
            "theft_description": stolen.get("theft_description"),
            "locking_description": stolen.get("locking_description"),
            "lock_defeat_description": stolen.get("lock_defeat_description"),
        }

    def collect(
        self,
        spec: BikeIndexDatasetSpec,
        batch_size: int = 50,
    ) -> dict:
        """Collect and ingest stolen bikes defined by a BikeIndexDatasetSpec.

        Parameters
        ----------
        spec : BikeIndexDatasetSpec
        batch_size : int
            Number of records per write_batch call.

        Returns
        -------
        dict
            Summary with counts of rows staged/merged and any errors.
        """
        summary = {
            "spec_name": spec.name,
            "total_rows_staged": 0,
            "total_rows_merged": 0,
            "errors": [],
        }

        search = BikeIndexSearchParams(**spec.to_search_params())
        dataset_id = f"{spec.name}/{spec.location}"

        if self.tracker:
            with self.tracker.track(
                source="bike_index_api",
                dataset_id=dataset_id,
                target_table=f"{spec.target_schema}.{spec.target_table}",
                metadata={
                    "location": spec.location,
                    "distance": spec.distance,
                    "stolenness": spec.stolenness,
                    "fetch_detail": spec.fetch_detail,
                },
            ) as run:
                staged, merged = self._collect_and_ingest(spec, search, batch_size)
                run.rows_staged = staged
                run.rows_merged = merged
                summary["total_rows_staged"] = staged
                summary["total_rows_merged"] = merged
        else:
            staged, merged = self._collect_and_ingest(spec, search, batch_size)
            summary["total_rows_staged"] = staged
            summary["total_rows_merged"] = merged

        logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    def _collect_and_ingest(
        self,
        spec: BikeIndexDatasetSpec,
        search: BikeIndexSearchParams,
        batch_size: int,
    ) -> tuple[int, int]:
        """Fetch all pages and ingest via StagedIngest.

        Returns (rows_staged, rows_merged).
        """
        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            entity_key=spec.entity_key,
            metadata_columns=METADATA_COLUMNS,
        ) as stager:
            batch: list[dict] = []
            total = 0

            for page_bikes in self.client.search_all(search):
                for summary in page_bikes:
                    detail = None
                    if spec.fetch_detail:
                        bike_id = summary.get("id")
                        if bike_id:
                            try:
                                detail = self.client.get_bike(bike_id)
                            except Exception:
                                logger.warning(
                                    "Failed to fetch detail for bike %s, skipping detail",
                                    bike_id,
                                    exc_info=True,
                                )

                    row = self._flatten_bike(summary, detail)
                    batch.append(row)
                    total += 1

                    if len(batch) >= batch_size:
                        logger.info("Writing batch of %d records (total: %d)", len(batch), total)
                        stager.write_batch(batch)
                        batch = []

            if batch:
                logger.info("Writing final batch of %d records (total: %d)", len(batch), total)
                stager.write_batch(batch)

        logger.info(
            "Ingested %s: staged=%d merged=%d", spec.name, stager.rows_staged, stager.rows_merged
        )
        return stager.rows_staged, stager.rows_merged

    def generate_ddl(self, spec: BikeIndexDatasetSpec) -> str:
        """Generate a CREATE TABLE statement for the bike index target table."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        entity_key = spec.entity_key or ["id"]
        ek_cols = ", ".join(f'"{c}"' for c in entity_key)

        columns = [
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
            '"latitude" double precision',
            '"longitude" double precision',
            '"stolen_location" text',
            '"theft_description" text',
            '"locking_description" text',
            '"lock_defeat_description" text',
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

# The Bike Index search endpoint returns summary data per bike. To get
# latitude/longitude for each theft, we need a second call to /bikes/{id}
# for each bike (the stolen_record with coordinates is only on the detail
# endpoint). This is the main bottleneck — one HTTP call per bike.

# Strategy:
#     1. Use search_all() to paginate through stolen bikes near a location.
#     2. For each bike in the search results, call get_bike() to get the
#        stolen_record with lat/lon (controlled by spec.fetch_detail).
#     3. Flatten into a dict matching the target schema.
#     4. Write batches via StagedIngest.

# Usage:
#     from bike_index.client import BikeIndexClient
#     from bike_index.spec import BikeIndexDatasetSpec
#     from bike_index.collector import BikeIndexCollector

#     spec = BikeIndexDatasetSpec(
#         name="chicago_stolen_bikes",
#         target_table="stolen_bikes",
#         location="Chicago, IL",
#         distance=10,
#         entity_key=["id"],
#     )

#     client = BikeIndexClient(access_token="...")
#     collector = BikeIndexCollector(client=client, engine=engine)
#     summary = collector.collect(spec)
# """

# import logging

# from loci.collectors.bike_index.client import BikeIndexClient, BikeIndexSearchParams
# from loci.collectors.bike_index.spec import BikeIndexDatasetSpec

# logger = logging.getLogger(__name__)

# METADATA_COLUMNS = {
#     "ingested_at",
#     "record_hash",
#     "valid_from",
#     "valid_to",
# }


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

#     def __init__(self, client, engine, tracker=None):
#         self.client = client
#         self.engine = engine
#         self.tracker = tracker

#     def collect(
#         self,
#         spec: BikeIndexDatasetSpec,
#         batch_size: int = 50,
#     ) -> dict:
#         """Collect and ingest stolen bikes defined by a BikeIndexDatasetSpec.

#         Parameters
#         ----------
#         spec : BikeIndexDatasetSpec
#         batch_size : int
#             Number of records per write_batch call.

#         Returns
#         -------
#         dict
#             Summary with counts of rows staged/merged and any errors.
#         """
#         summary = {
#             "spec_name": spec.name,
#             "total_rows_staged": 0,
#             "total_rows_merged": 0,
#             "errors": [],
#         }

#         search = BikeIndexSearchParams(**spec.to_search_params())
#         dataset_id = f"{spec.name}/{spec.location}"

#         if self.tracker:
#             with self.tracker.track(
#                 source="bike_index_api",
#                 dataset_id=dataset_id,
#                 target_table=f"{spec.target_schema}.{spec.target_table}",
#                 metadata={
#                     "location": spec.location,
#                     "distance": spec.distance,
#                     "stolenness": spec.stolenness,
#                     "fetch_detail": spec.fetch_detail,
#                 },
#             ) as run:
#                 staged, merged = self._collect_and_ingest(spec, search, batch_size)
#                 run.rows_staged = staged
#                 run.rows_merged = merged
#                 summary["total_rows_staged"] = staged
#                 summary["total_rows_merged"] = merged
#         else:
#             staged, merged = self._collect_and_ingest(spec, search, batch_size)
#             summary["total_rows_staged"] = staged
#             summary["total_rows_merged"] = merged

#         logger.info("Collection complete for %r: %s", spec.name, summary)
#         return summary

#     def _flatten_bike(self, summary: dict, detail: dict | None) -> dict:
#         """Flatten a bike summary + detail response into a single row dict.

#         Returns a dict with a stable set of keys suitable for insertion into
#         a Postgres table. Coordinates come from the detail endpoint's
#         stolen_record; if detail is unavailable, lat/lon will be None.
#         """
#         stolen = (detail or {}).get("stolen_record") or {}

#         return {
#             "id": summary.get("id"),
#             "title": summary.get("title"),
#             "serial": summary.get("serial"),
#             "manufacturer_name": summary.get("manufacturer_name"),
#             "frame_model": summary.get("frame_model"),
#             "frame_colors": summary.get("frame_colors"),
#             "year": summary.get("year"),
#             "stolen": summary.get("stolen", True),
#             "date_stolen": summary.get("date_stolen"),
#             "description": summary.get("description"),
#             "thumb": summary.get("thumb"),
#             "url": summary.get("url"),
#             # From the detail endpoint's stolen_record
#             "latitude": stolen.get("latitude"),
#             "longitude": stolen.get("longitude"),
#             "theft_description": stolen.get("theft_description"),
#             "locking_description": stolen.get("locking_description"),
#             "lock_defeat_description": stolen.get("lock_defeat_description"),
#             "location_found": stolen.get("location"),
#         }

#     def _collect_and_ingest(
#         self,
#         spec: BikeIndexDatasetSpec,
#         search: BikeIndexSearchParams,
#         batch_size: int,
#     ) -> tuple[int, int]:
#         """Fetch all pages and ingest via StagedIngest.

#         Returns (rows_staged, rows_merged).
#         """
#         with self.engine.staged_ingest(
#             target_table=spec.target_table,
#             target_schema=spec.target_schema,
#             entity_key=spec.entity_key,
#             metadata_columns=METADATA_COLUMNS,
#         ) as stager:
#             batch: list[dict] = []
#             total = 0

#             for page_bikes in self.client.search_all(search):
#                 for summary in page_bikes:
#                     detail = None
#                     if spec.fetch_detail:
#                         bike_id = summary.get("id")
#                         if bike_id:
#                             try:
#                                 detail = self.client.get_bike(bike_id)
#                             except Exception:
#                                 logger.warning(
#                                     "Failed to fetch detail for bike %s, skipping detail",
#                                     bike_id,
#                                     exc_info=True,
#                                 )

#                     row = self._flatten_bike(summary, detail)
#                     batch.append(row)
#                     total += 1

#                     if len(batch) >= batch_size:
#                         logger.info("Writing batch of %d records (total: %d)", len(batch), total)
#                         stager.write_batch(batch)
#                         batch = []

#             if batch:
#                 logger.info("Writing final batch of %d records (total: %d)", len(batch), total)
#                 stager.write_batch(batch)

#         logger.info("Ingested %s: staged=%d merged=%d", spec.name, stager.rows_staged, stager.rows_merged)
#         return stager.rows_staged, stager.rows_merged

#     def generate_ddl(self, spec: BikeIndexDatasetSpec) -> str:
#         """Generate a CREATE TABLE statement for the bike index target table."""
#         fqn = f"{spec.target_schema}.{spec.target_table}"
#         entity_key = spec.entity_key or ["id"]
#         ek_cols = ", ".join(f'"{c}"' for c in entity_key)

#         columns = [
#             '"id" integer not null',
#             '"title" text',
#             '"serial" text',
#             '"manufacturer_name" text',
#             '"frame_model" text',
#             '"frame_colors" text[]',
#             '"year" integer',
#             '"stolen" boolean',
#             '"date_stolen" bigint',
#             '"description" text',
#             '"thumb" text',
#             '"url" text',
#             '"latitude" double precision',
#             '"longitude" double precision',
#             '"theft_description" text',
#             '"locking_description" text',
#             '"lock_defeat_description" text',
#             '"location_found" text',
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
