"""TIGER geocoder wrapper for populating a geocoding cache table.

Uses PostGIS TIGER's normalize_address() and geocode() functions to
geocode addresses that are missing lat/lng in the cache. Tracks failed
attempts so that permanent failures (bad ratings, no results) are not
retried, while transient errors (timeouts, connection issues) are.

Usage:
    from your_module import PostgresEngine
    from tiger_geocoder import TigerGeocoder

    engine = PostgresEngine(...)
    geocoder = TigerGeocoder(engine)
    geocoder.process_all()
"""

import logging

from loci.db.core import PostgresEngine

logger = logging.getLogger(__name__)


class TigerGeocoder:
    """Geocodes pending addresses in a cache table using PostGIS TIGER.

    Args:
        engine: A PostgresEngine instance with .query() and .execute() methods.
        table_name: Fully qualified name of the geocoding cache table.
        rating_threshold: Maximum acceptable TIGER rating (0 = perfect, higher = worse).
            Results above this threshold are recorded as permanent failures.
        batch_size: Number of rows to fetch and process per batch.
        tiger_data_year: The TIGER data vintage string for audit metadata.
        restrict_region: An optional input to limit the area the geocoder searches for matches.
            Use "ST_MakeEnvelope(-87.94, 41.64, -87.52, 42.03, 4269)" to limit to Chicagoland.
    """

    PERMANENT_PREFIX_NO_RESULT = "no_result"
    PERMANENT_PREFIX_BELOW_THRESHOLD = "below_threshold"
    TRANSIENT_PREFIX = "error"

    def __init__(
        self,
        engine: PostgresEngine,
        schema_name: str,
        table_name: str = "geocoded_address_cache",
        rating_threshold: int = 20,
        batch_size: int = 100,
        tiger_data_year: str = "2025",
        statement_timeout: str = "30s",
        restrict_region: str | None = None,
    ):
        self.engine = engine
        self.schema_name = schema_name
        self.table_name = table_name
        self.rating_threshold = rating_threshold
        self.batch_size = batch_size
        self.tiger_data_year = tiger_data_year
        self.statement_timeout = statement_timeout
        self.restrict_region = restrict_region

    def normalize(self, address_string: str) -> dict | None:
        """Parse an address string using TIGER's normalize_address().

        Args:
            address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

        Returns:
            A dict with TIGER's parsed address parts, or None if normalization fails.
        """
        try:
            result = self.engine.query(
                """
                SELECT
                    (n).address::text     AS tiger_address_num,
                    (n).predirabbrev      AS tiger_predir,
                    (n).streetname        AS tiger_street_name,
                    (n).streettypeabbrev  AS tiger_street_type,
                    (n).postdirabbrev     AS tiger_postdir,
                    (n).location          AS tiger_city,
                    (n).stateabbrev       AS tiger_state,
                    (n).zip               AS tiger_zip
                FROM normalize_address(%s) AS n
                """,
                (address_string,),
                as_dicts=True,
            )
        except Exception as e:
            logger.warning("normalize_address() failed for %r: %s", address_string, e)
            return None

        if not result:
            return None

        return result[0]

    def geocode(self, address_string: str) -> dict | None:
        """Geocode an address string using TIGER's geocode().

        Args:
            address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

        Returns:
            A dict with keys: rating, latitude, longitude, geom. Or None if
            geocode() returns no results.
        """
        try:
            if self.restrict_region is not None:
                geocode_cmd = f"geocode(%s, 1, {self.restrict_region})"
            else:
                geocode_cmd = "geocode(%s, 1)"
            result = self.engine.query(
                f"""
                select
                    g.rating,
                    ST_Y(g.geomout)  as latitude,
                    ST_X(g.geomout)  as longitude,
                    g.geomout        as geom
                from {geocode_cmd} as g """,
                params=(address_string,),
                as_dicts=True,
            )
        except Exception as e:
            logger.warning("geocode() failed for %r: %s", address_string, e)
            raise

        if not result:
            return None

        row = result[0]
        return {
            "rating": row["rating"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "geom": row["geom"],
        }

    def get_pending_batch(self) -> list[dict]:
        """Fetch the next batch of addresses that need geocoding.

        Returns rows where:
        - latitude is NULL (not yet geocoded), AND
        - either never attempted, or the last attempt was a transient error.

        Each call returns a fresh batch, excluding rows that were updated
        by previous calls in the same process_all() run.
        """
        return self.engine.query(
            f"""
            SELECT address_hash, raw_address, street, city, state, zip
            FROM {self.schema_name}.{self.table_name}
            WHERE latitude IS NULL
              AND (
                  geocode_fail_reason IS NULL
                  OR geocode_fail_reason LIKE '{self.TRANSIENT_PREFIX}:%%'
              )
            LIMIT %s
            """,
            (self.batch_size,),
            as_dicts=True,
        )

    def _build_geocode_input(self, row: dict) -> str:
        """Build the address string to pass to geocode().

        Uses structured parts if available, otherwise falls back to raw_address.

        Args:
            row: A dict with keys: street, city, state, zip, raw_address.

        Returns:
            An address string suitable for geocode().
        """
        if row.get("street") and row.get("state"):
            parts = [row["street"]]
            if row.get("city"):
                parts.append(row["city"])
            parts.append(row["state"])
            if row.get("zip"):
                parts[-1] = f"{row['state']} {row['zip']}"
            return ", ".join(parts)
        return row["raw_address"]

    def _update_success(
        self,
        address_hash: str,
        geocode_result: dict,
        normalized_parts: dict | None,
        geocode_input: str,
    ) -> None:
        """Write a successful geocoding result to the cache.

        Args:
            address_hash: The row's address_hash PK.
            geocode_result: Dict with rating, latitude, longitude, geom.
            normalized_parts: Dict from normalize(), or None.
            geocode_input: The exact string passed to geocode().
        """
        parts = normalized_parts or {}
        self.engine.execute(
            f"""
            UPDATE {self.schema_name}.{self.table_name}
            SET latitude = %s,
                longitude = %s,
                geom = %s,
                geocode_rating = %s,
                geocode_source = 'tiger',
                rating_threshold_used = %s,
                normalized_input = %s,
                tiger_data_year = %s,
                tiger_address_num = %s,
                tiger_predir = %s,
                tiger_street_name = %s,
                tiger_street_type = %s,
                tiger_postdir = %s,
                tiger_city = %s,
                tiger_state = %s,
                tiger_zip = %s,
                geocoded_at = now(),
                geocode_attempted_at = now(),
                geocode_fail_reason = NULL
            WHERE address_hash = %s
            """,
            (
                geocode_result["latitude"],
                geocode_result["longitude"],
                geocode_result["geom"],
                geocode_result["rating"],
                self.rating_threshold,
                geocode_input,
                self.tiger_data_year,
                parts.get("tiger_address_num"),
                parts.get("tiger_predir"),
                parts.get("tiger_street_name"),
                parts.get("tiger_street_type"),
                parts.get("tiger_postdir"),
                parts.get("tiger_city"),
                parts.get("tiger_state"),
                parts.get("tiger_zip"),
                address_hash,
            ),
        )

    def _update_failure(self, address_hash: str, reason: str) -> None:
        """Record a failed geocoding attempt.

        Args:
            address_hash: The row's address_hash PK.
            reason: A failure reason string. Strings starting with 'error:'
                are treated as transient (will be retried). All others are
                treated as permanent.
        """
        self.engine.execute(
            f"""
            UPDATE {self.schema_name}.{self.table_name}
            SET geocode_attempted_at = now(),
                geocode_fail_reason = %s
            WHERE address_hash = %s
            """,
            (reason, address_hash),
        )

    def process_row(self, row: dict) -> str:
        """Normalize and geocode a single row, updating the cache accordingly.

        Args:
            row: A dict from get_pending_batch().

        Returns:
            One of: 'success', 'permanent_failure', 'transient_failure'.
        """
        address_hash = row["address_hash"]
        geocode_input = self._build_geocode_input(row)

        normalized_parts = self.normalize(geocode_input)

        try:
            result = self.geocode(geocode_input)
        except Exception as e:
            self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
            return "transient_failure"

        if result is None:
            self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
            logger.info("No result for %s (%s)", address_hash, geocode_input)
            return "permanent_failure"

        if result["rating"] > self.rating_threshold:
            reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
            self._update_failure(address_hash, reason)
            logger.info(
                "Below threshold for %s (%s): rating %d > %d",
                address_hash,
                geocode_input,
                result["rating"],
                self.rating_threshold,
            )
            return "permanent_failure"

        self._update_success(address_hash, result, normalized_parts, geocode_input)
        logger.debug(
            "Geocoded %s (%s): rating %d",
            address_hash,
            geocode_input,
            result["rating"],
        )
        return "success"

    def process_all(self) -> dict:
        """Process all pending addresses in batches.

        Fetches a batch, processes each row, then fetches the next batch.
        Each batch is a self-contained query so there are no long-lived
        cursors or transactions. Results are written immediately so
        progress is not lost if the process is interrupted.

        Returns:
            A summary dict with counts of successes, permanent failures,
            transient failures, and total processed.
        """
        self.engine.execute(f"SET statement_timeout = '{self.statement_timeout}'")
        try:
            counts = {
                "success": 0,
                "permanent_failure": 0,
                "transient_failure": 0,
            }

            while True:
                batch = self.get_pending_batch()
                if not batch:
                    break

                logger.info("Processing batch of %d addresses", len(batch))

                for row in batch:
                    outcome = self.process_row(row)
                    counts[outcome] += 1

            total = sum(counts.values())
            logger.info(
                "Geocoding complete: %d total, %d successes, %d permanent failures, %d transient failures",
                total,
                counts["success"],
                counts["permanent_failure"],
                counts["transient_failure"],
            )

            return {"total": total, **counts}
        finally:
            self.engine.execute("reset statement_timeout")

    # def normalize(self, address_string: str) -> dict | None:
    #     """Parse an address string using TIGER's normalize_address().

    #     Args:
    #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

    #     Returns:
    #         A dict with TIGER's parsed address parts, or None if normalization fails.
    #     """
    #     try:
    #         result = self.engine.query(
    #             """
    #             SELECT
    #                 (n).address::text     AS tiger_address_num,
    #                 (n).predirabbrev      AS tiger_predir,
    #                 (n).streetname        AS tiger_street_name,
    #                 (n).streettypeabbrev  AS tiger_street_type,
    #                 (n).postdirabbrev     AS tiger_postdir,
    #                 (n).location          AS tiger_city,
    #                 (n).stateabbrev       AS tiger_state,
    #                 (n).zip               AS tiger_zip
    #             FROM normalize_address(%s) AS n
    #             """,
    #             (address_string,),
    #             as_dicts=True,
    #         )
    #     except Exception as e:
    #         logger.warning("normalize_address() failed for %r: %s", address_string, e)
    #         return None

    #     if not result:
    #         return None

    #     return result[0]

    # def geocode(self, address_string: str) -> dict | None:
    #     """Geocode an address string using TIGER's geocode().

    #     Args:
    #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

    #     Returns:
    #         A dict with keys: rating, latitude, longitude, geom. Or None if
    #         geocode() returns no results.
    #     """
    #     try:
    #         result = self.engine.query(
    #             """
    #             SELECT
    #                 g.rating,
    #                 ST_Y(g.geomout)  AS latitude,
    #                 ST_X(g.geomout)  AS longitude,
    #                 g.geomout        AS geom
    #             FROM geocode(%s, 1) AS g
    #             """,
    #             (address_string,),
    #             as_dicts=True,
    #         )
    #     except Exception as e:
    #         logger.warning("geocode() failed for %r: %s", address_string, e)
    #         raise

    #     if not result:
    #         return None

    #     row = result[0]
    #     return {
    #         "rating": row["rating"],
    #         "latitude": row["latitude"],
    #         "longitude": row["longitude"],
    #         "geom": row["geom"],
    #     }

    # def pending_rows(self) -> "Iterator[list[dict]]":
    #     """Yield batches of addresses that need geocoding via server-side cursor.

    #     Yields batches of row dicts where:
    #     - latitude is NULL (not yet geocoded), AND
    #     - either never attempted, or the last attempt was a transient error.
    #     """
    #     return self.engine.query_batches(
    #         f"""
    #         SELECT address_hash, raw_address, street, city, state, zip
    #         FROM {self.schema_name}.{self.table_name}
    #         WHERE latitude IS NULL
    #           AND (
    #               geocode_fail_reason IS NULL
    #               OR geocode_fail_reason LIKE '{self.TRANSIENT_PREFIX}:%%'
    #           )
    #         """,
    #         batch_size=self.batch_size,
    #         as_dicts=True,
    #     )

    # def _build_geocode_input(self, row: dict) -> str:
    #     """Build the address string to pass to geocode().

    #     Uses structured parts if available, otherwise falls back to raw_address.

    #     Args:
    #         row: A dict with keys: street, city, state, zip, raw_address.

    #     Returns:
    #         An address string suitable for geocode().
    #     """
    #     if row.get("street") and row.get("state"):
    #         parts = [row["street"]]
    #         if row.get("city"):
    #             parts.append(row["city"])
    #         parts.append(row["state"])
    #         if row.get("zip"):
    #             parts[-1] = f"{row['state']} {row['zip']}"
    #         return ", ".join(parts)
    #     return row["raw_address"]

    # def _update_success(
    #     self,
    #     address_hash: str,
    #     geocode_result: dict,
    #     normalized_parts: dict | None,
    #     geocode_input: str,
    # ) -> None:
    #     """Write a successful geocoding result to the cache.

    #     Args:
    #         address_hash: The row's address_hash PK.
    #         geocode_result: Dict with rating, latitude, longitude, geom.
    #         normalized_parts: Dict from normalize(), or None.
    #         geocode_input: The exact string passed to geocode().
    #     """
    #     parts = normalized_parts or {}
    #     self.engine.execute(
    #         f"""
    #         UPDATE {self.schema_name}.{self.table_name}
    #         SET latitude = %s,
    #             longitude = %s,
    #             geom = %s,
    #             geocode_rating = %s,
    #             geocode_source = 'tiger',
    #             rating_threshold_used = %s,
    #             normalized_input = %s,
    #             tiger_data_year = %s,
    #             tiger_address_num = %s,
    #             tiger_predir = %s,
    #             tiger_street_name = %s,
    #             tiger_street_type = %s,
    #             tiger_postdir = %s,
    #             tiger_city = %s,
    #             tiger_state = %s,
    #             tiger_zip = %s,
    #             geocoded_at = now(),
    #             geocode_attempted_at = now(),
    #             geocode_fail_reason = NULL
    #         WHERE address_hash = %s
    #         """,
    #         (
    #             geocode_result["latitude"],
    #             geocode_result["longitude"],
    #             geocode_result["geom"],
    #             geocode_result["rating"],
    #             self.rating_threshold,
    #             geocode_input,
    #             self.tiger_data_year,
    #             parts.get("tiger_address_num"),
    #             parts.get("tiger_predir"),
    #             parts.get("tiger_street_name"),
    #             parts.get("tiger_street_type"),
    #             parts.get("tiger_postdir"),
    #             parts.get("tiger_city"),
    #             parts.get("tiger_state"),
    #             parts.get("tiger_zip"),
    #             address_hash,
    #         ),
    #     )

    # def _update_failure(self, address_hash: str, reason: str) -> None:
    #     """Record a failed geocoding attempt.

    #     Args:
    #         address_hash: The row's address_hash PK.
    #         reason: A failure reason string. Strings starting with 'error:'
    #             are treated as transient (will be retried). All others are
    #             treated as permanent.
    #     """
    #     self.engine.execute(
    #         f"""
    #         UPDATE {self.schema_name}.{self.table_name}
    #         SET geocode_attempted_at = now(),
    #             geocode_fail_reason = %s
    #         WHERE address_hash = %s
    #         """,
    #         (reason, address_hash),
    #     )

    # def process_row(self, row: dict) -> str:
    #     """Normalize and geocode a single row, updating the cache accordingly.

    #     Args:
    #         row: A dict from get_pending_batch().

    #     Returns:
    #         One of: 'success', 'permanent_failure', 'transient_failure'.
    #     """
    #     address_hash = row["address_hash"]
    #     geocode_input = self._build_geocode_input(row)

    #     normalized_parts = self.normalize(geocode_input)

    #     try:
    #         result = self.geocode(geocode_input)
    #     except Exception as e:
    #         self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
    #         return "transient_failure"

    #     if result is None:
    #         self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
    #         logger.info("No result for %s (%s)", address_hash, geocode_input)
    #         return "permanent_failure"

    #     if result["rating"] > self.rating_threshold:
    #         reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
    #         self._update_failure(address_hash, reason)
    #         logger.info(
    #             "Below threshold for %s (%s): rating %d > %d",
    #             address_hash,
    #             geocode_input,
    #             result["rating"],
    #             self.rating_threshold,
    #         )
    #         return "permanent_failure"

    #     self._update_success(address_hash, result, normalized_parts, geocode_input)
    #     logger.debug(
    #         "Geocoded %s (%s): rating %d",
    #         address_hash,
    #         geocode_input,
    #         result["rating"],
    #     )
    #     return "success"

    # def process_all(self) -> dict:
    #     """Process all pending addresses in batches.

    #     Uses a server-side cursor to stream pending rows in batches.
    #     Each row is processed and written immediately so progress is not
    #     lost if the process is interrupted.

    #     Returns:
    #         A summary dict with counts of successes, permanent failures,
    #         transient failures, and total processed.
    #     """
    #     counts = {
    #         "success": 0,
    #         "permanent_failure": 0,
    #         "transient_failure": 0,
    #     }

    #     for batch in self.pending_rows():
    #         logger.info("Processing batch of %d addresses", len(batch))

    #         for row in batch:
    #             outcome = self.process_row(row)
    #             counts[outcome] += 1

    #     total = sum(counts.values())
    #     logger.info(
    #         "Geocoding complete: %d total, %d successes, %d permanent failures, %d transient failures",
    #         total,
    #         counts["success"],
    #         counts["permanent_failure"],
    #         counts["transient_failure"],
    #     )

    #     return {"total": total, **counts}


# class TigerGeocoder:
#     """Geocodes pending addresses in a cache table using PostGIS TIGER.

#     Args:
#         engine: A PostgresEngine instance with .query() and .execute() methods.
#         table_name: Fully qualified name of the geocoding cache table.
#         rating_threshold: Maximum acceptable TIGER rating (0 = perfect, higher = worse).
#             Results above this threshold are recorded as permanent failures.
#         batch_size: Number of rows to fetch and process per batch.
#         tiger_data_year: The TIGER data vintage string for audit metadata.
#     """

#     PERMANENT_PREFIX_NO_RESULT = "no_result"
#     PERMANENT_PREFIX_BELOW_THRESHOLD = "below_threshold"
#     TRANSIENT_PREFIX = "error"

#     def __init__(
#         self,
#         engine: PostgresEngine,
#         schema_name: str,
#         table_name: str = "geocoded_address_cache",
#         rating_threshold: int = 20,
#         batch_size: int = 100,
#         tiger_data_year: str = "2025",
#     ):
#         self.engine = engine
#         self.schema_name = schema_name
#         self.table_name = table_name
#         self.rating_threshold = rating_threshold
#         self.batch_size = batch_size
#         self.tiger_data_year = tiger_data_year

#     def normalize(self, address_string: str) -> dict | None:
#         """Parse an address string using TIGER's normalize_address().

#         Args:
#             address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

#         Returns:
#             A dict with TIGER's parsed address parts, or None if normalization fails.
#         """
#         try:
#             result = self.engine.query(
#                 """
#                 SELECT
#                     (n).address::text     AS tiger_address_num,
#                     (n).predirabbrev      AS tiger_predir,
#                     (n).streetname        AS tiger_street_name,
#                     (n).streettypeabbrev  AS tiger_street_type,
#                     (n).postdirabbrev     AS tiger_postdir,
#                     (n).location          AS tiger_city,
#                     (n).stateabbrev       AS tiger_state,
#                     (n).zip               AS tiger_zip
#                 FROM normalize_address(%s) AS n
#                 """,
#                 [address_string],
#             )
#         except Exception as e:
#             logger.warning("normalize_address() failed for %r: %s", address_string, e)
#             return None

#         if result.empty:
#             return None

#         return result.iloc[0].to_dict()

#     # def normalize(self, address_string: str) -> dict | None:
#     #     """Parse an address string using TIGER's normalize_address().

#     #     Args:
#     #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

#     #     Returns:
#     #         A dict with TIGER's parsed address parts, or None if normalization fails.
#     #     """
#     #     try:
#     #         result = self.engine.query(
#     #             """
#     #             SELECT
#     #                 (n).address::text     AS tiger_address_num,
#     #                 (n).predirabbrev      AS tiger_predir,
#     #                 (n).streetname        AS tiger_street_name,
#     #                 (n).streettypeabbrev  AS tiger_street_type,
#     #                 (n).postdirabbrev     AS tiger_postdir,
#     #                 (n).location          AS tiger_city,
#     #                 (n).stateabbrev       AS tiger_state,
#     #                 (n).zip               AS tiger_zip
#     #             FROM normalize_address(%s) AS n
#     #             """,
#     #             [address_string],
#     #         )
#     #     except Exception as e:
#     #         logger.warning("normalize_address() failed for %r: %s", address_string, e)
#     #         return None

#     #     if not result:
#     #         return None

#     #     return dict(result[0])

#     def geocode(self, address_string: str) -> dict | None:
#         """Geocode an address string using TIGER's geocode().

#         Args:
#             address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

#         Returns:
#             A dict with keys: rating, latitude, longitude, geom. Or None if
#             geocode() returns no results.
#         """
#         try:
#             result = self.engine.query(
#                 """
#                 SELECT
#                     g.rating,
#                     ST_Y(g.geomout)  AS latitude,
#                     ST_X(g.geomout)  AS longitude,
#                     g.geomout        AS geom
#                 FROM geocode(%s, 1) AS g
#                 """,
#                 [address_string],
#             )
#         except Exception as e:
#             logger.warning("geocode() failed for %r: %s", address_string, e)
#             raise

#         if result.empty:
#             return None

#         row = result.iloc[0]
#         return {
#             "rating": row["rating"],
#             "latitude": row["latitude"],
#             "longitude": row["longitude"],
#             "geom": row["geom"],
#         }

#     # def geocode(self, address_string: str) -> dict | None:
#     #     """Geocode an address string using TIGER's geocode().

#     #     Args:
#     #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

#     #     Returns:
#     #         A dict with keys: rating, latitude, longitude, geom. Or None if
#     #         geocode() returns no results.
#     #     """
#     #     try:
#     #         result = self.engine.query(
#     #             """
#     #             SELECT
#     #                 g.rating,
#     #                 ST_Y(g.geomout)  AS latitude,
#     #                 ST_X(g.geomout)  AS longitude,
#     #                 g.geomout        AS geom
#     #             FROM geocode(%s, 1) AS g
#     #             """,
#     #             [address_string],
#     #         )
#     #     except Exception as e:
#     #         logger.warning("geocode() failed for %r: %s", address_string, e)
#     #         raise

#     #     if not result:
#     #         return None

#     #     row = result[0]
#     #     return {
#     #         "rating": row["rating"],
#     #         "latitude": row["latitude"],
#     #         "longitude": row["longitude"],
#     #         "geom": row["geom"],
#     #     }

#     def pending_rows(self) -> "Iterator[list[dict]]":
#         """Yield batches of addresses that need geocoding via server-side cursor.

#         Yields batches of row dicts where:
#         - latitude is NULL (not yet geocoded), AND
#         - either never attempted, or the last attempt was a transient error.
#         """
#         return self.engine.query_batches(
#             f"""
#             SELECT address_hash, raw_address, street, city, state, zip
#             FROM {self.schema_name}.{self.table_name}
#             WHERE latitude IS NULL
#               AND (
#                   geocode_fail_reason IS NULL
#                   OR geocode_fail_reason LIKE '{self.TRANSIENT_PREFIX}:%%'
#               )
#             """,
#             batch_size=self.batch_size,
#             as_dicts=True,
#         )

#     def _build_geocode_input(self, row: dict) -> str:
#         """Build the address string to pass to geocode().

#         Uses structured parts if available, otherwise falls back to raw_address.

#         Args:
#             row: A dict with keys: street, city, state, zip, raw_address.

#         Returns:
#             An address string suitable for geocode().
#         """
#         if row.get("street") and row.get("state"):
#             parts = [row["street"]]
#             if row.get("city"):
#                 parts.append(row["city"])
#             parts.append(row["state"])
#             if row.get("zip"):
#                 parts[-1] = f"{row['state']} {row['zip']}"
#             return ", ".join(parts)
#         return row["raw_address"]

#     def _update_success(
#         self,
#         address_hash: str,
#         geocode_result: dict,
#         normalized_parts: dict | None,
#         geocode_input: str,
#     ) -> None:
#         """Write a successful geocoding result to the cache.

#         Args:
#             address_hash: The row's address_hash PK.
#             geocode_result: Dict with rating, latitude, longitude, geom.
#             normalized_parts: Dict from normalize(), or None.
#             geocode_input: The exact string passed to geocode().
#         """
#         parts = normalized_parts or {}
#         self.engine.execute(
#             f"""
#             UPDATE {self.schema_name}.{self.table_name}
#             SET latitude = %s,
#                 longitude = %s,
#                 geom = %s,
#                 geocode_rating = %s,
#                 geocode_source = 'tiger',
#                 rating_threshold_used = %s,
#                 normalized_input = %s,
#                 tiger_data_year = %s,
#                 tiger_address_num = %s,
#                 tiger_predir = %s,
#                 tiger_street_name = %s,
#                 tiger_street_type = %s,
#                 tiger_postdir = %s,
#                 tiger_city = %s,
#                 tiger_state = %s,
#                 tiger_zip = %s,
#                 geocoded_at = now(),
#                 geocode_attempted_at = now(),
#                 geocode_fail_reason = NULL
#             WHERE address_hash = %s
#             """,
#             [
#                 geocode_result["latitude"],
#                 geocode_result["longitude"],
#                 geocode_result["geom"],
#                 geocode_result["rating"],
#                 self.rating_threshold,
#                 geocode_input,
#                 self.tiger_data_year,
#                 parts.get("tiger_address_num"),
#                 parts.get("tiger_predir"),
#                 parts.get("tiger_street_name"),
#                 parts.get("tiger_street_type"),
#                 parts.get("tiger_postdir"),
#                 parts.get("tiger_city"),
#                 parts.get("tiger_state"),
#                 parts.get("tiger_zip"),
#                 address_hash,
#             ],
#         )

#     def _update_failure(self, address_hash: str, reason: str) -> None:
#         """Record a failed geocoding attempt.

#         Args:
#             address_hash: The row's address_hash PK.
#             reason: A failure reason string. Strings starting with 'error:'
#                 are treated as transient (will be retried). All others are
#                 treated as permanent.
#         """
#         self.engine.execute(
#             f"""
#             UPDATE {self.schema_name}.{self.table_name}
#             SET geocode_attempted_at = now(),
#                 geocode_fail_reason = %s
#             WHERE address_hash = %s
#             """,
#             [reason, address_hash],
#         )

#     def process_row(self, row: dict) -> str:
#         """Normalize and geocode a single row, updating the cache accordingly.

#         Args:
#             row: A dict from get_pending_batch().

#         Returns:
#             One of: 'success', 'permanent_failure', 'transient_failure'.
#         """
#         address_hash = row["address_hash"]
#         geocode_input = self._build_geocode_input(row)

#         normalized_parts = self.normalize(geocode_input)

#         try:
#             result = self.geocode(geocode_input)
#         except Exception as e:
#             self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
#             return "transient_failure"

#         if result is None:
#             self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
#             logger.info("No result for %s (%s)", address_hash, geocode_input)
#             return "permanent_failure"

#         if result["rating"] > self.rating_threshold:
#             reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
#             self._update_failure(address_hash, reason)
#             logger.info(
#                 "Below threshold for %s (%s): rating %d > %d",
#                 address_hash,
#                 geocode_input,
#                 result["rating"],
#                 self.rating_threshold,
#             )
#             return "permanent_failure"

#         self._update_success(address_hash, result, normalized_parts, geocode_input)
#         logger.debug(
#             "Geocoded %s (%s): rating %d",
#             address_hash,
#             geocode_input,
#             result["rating"],
#         )
#         return "success"

#     def process_all(self) -> dict:
#         """Process all pending addresses in batches.

#         Uses a server-side cursor to stream pending rows in batches.
#         Each row is processed and written immediately so progress is not
#         lost if the process is interrupted.

#         Returns:
#             A summary dict with counts of successes, permanent failures,
#             transient failures, and total processed.
#         """
#         counts = {
#             "success": 0,
#             "permanent_failure": 0,
#             "transient_failure": 0,
#         }

#         for batch in self.pending_rows():
#             logger.info("Processing batch of %d addresses", len(batch))

#             for row in batch:
#                 outcome = self.process_row(row)
#                 counts[outcome] += 1

#         total = sum(counts.values())
#         logger.info(
#             "Geocoding complete: %d total, %d successes, %d permanent failures, %d transient failures",
#             total,
#             counts["success"],
#             counts["permanent_failure"],
#             counts["transient_failure"],
#         )

#         return {"total": total, **counts}

# #     def normalize(self, address_string: str) -> dict | None:
# #         """Parse an address string using TIGER's normalize_address().

# #         Args:
# #             address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

# #         Returns:
# #             A dict with TIGER's parsed address parts, or None if normalization fails.
# #         """
# #         try:
# #             result = self.engine.query(
# #                 """
# #                 SELECT
# #                     (n).address::text     AS tiger_address_num,
# #                     (n).predirabbrev      AS tiger_predir,
# #                     (n).streetname        AS tiger_street_name,
# #                     (n).streettypeabbrev  AS tiger_street_type,
# #                     (n).postdirabbrev     AS tiger_postdir,
# #                     (n).location          AS tiger_city,
# #                     (n).stateabbrev       AS tiger_state,
# #                     (n).zip               AS tiger_zip
# #                 FROM normalize_address(%s) AS n
# #                 """,
# #                 (address_string,),
# #             )
# #         except Exception as e:
# #             logger.warning("normalize_address() failed for %r: %s", address_string, e)
# #             return None

# #         if not result:
# #             return None

# #         return dict(result[0])

# #     def geocode(self, address_string: str) -> dict | None:
# #         """Geocode an address string using TIGER's geocode().

# #         Args:
# #             address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

# #         Returns:
# #             A dict with keys: rating, latitude, longitude, geom. Or None if
# #             geocode() returns no results.
# #         """
# #         try:
# #             result = self.engine.query(
# #                 """
# #                 SELECT
# #                     g.rating,
# #                     ST_Y(g.geomout)  AS latitude,
# #                     ST_X(g.geomout)  AS longitude,
# #                     g.geomout        AS geom
# #                 FROM geocode(%s, 1) AS g
# #                 """,
# #                 (address_string,),
# #             )
# #         except Exception as e:
# #             logger.warning("geocode() failed for %r: %s", address_string, e)
# #             raise

# #         if not result:
# #             return None

# #         row = result[0]
# #         return {
# #             "rating": row["rating"],
# #             "latitude": row["latitude"],
# #             "longitude": row["longitude"],
# #             "geom": row["geom"],
# #         }

# #     def get_pending_batch(self) -> list[dict]:
# #         """Fetch the next batch of addresses that need geocoding.

# #         Returns rows where:
# #         - latitude is NULL (not yet geocoded), AND
# #         - either never attempted, or the last attempt was a transient error.

# #         Returns:
# #             A list of row dicts with address_hash, raw_address, street,
# #             city, state, zip.
# #         """
# #         return self.engine.query(
# #             f"""
# #             SELECT address_hash, raw_address, street, city, state, zip
# #             FROM {self.schema_name}.{self.table_name}
# #             WHERE latitude IS NULL
# #               AND (
# #                   geocode_fail_reason IS NULL
# #                   OR geocode_fail_reason LIKE '{self.TRANSIENT_PREFIX}:%%'
# #               )
# #             LIMIT %s
# #             """,
# #             (self.batch_size,),
# #         )

# #     def _build_geocode_input(self, row: dict) -> str:
# #         """Build the address string to pass to geocode().

# #         Uses structured parts if available, otherwise falls back to raw_address.

# #         Args:
# #             row: A dict with keys: street, city, state, zip, raw_address.

# #         Returns:
# #             An address string suitable for geocode().
# #         """
# #         if row.get("street") and row.get("state"):
# #             parts = [row["street"]]
# #             if row.get("city"):
# #                 parts.append(row["city"])
# #             parts.append(row["state"])
# #             if row.get("zip"):
# #                 parts[-1] = f"{row['state']} {row['zip']}"
# #             return ", ".join(parts)
# #         return row["raw_address"]

# #     def _update_success(
# #         self,
# #         address_hash: str,
# #         geocode_result: dict,
# #         normalized_parts: dict | None,
# #         geocode_input: str,
# #     ) -> None:
# #         """Write a successful geocoding result to the cache.

# #         Args:
# #             address_hash: The row's address_hash PK.
# #             geocode_result: Dict with rating, latitude, longitude, geom.
# #             normalized_parts: Dict from normalize(), or None.
# #             geocode_input: The exact string passed to geocode().
# #         """
# #         parts = normalized_parts or {}
# #         self.engine.execute(
# #             f"""
# #             UPDATE {self.schema_name}.{self.table_name}
# #             SET latitude = %s,
# #                 longitude = %s,
# #                 geom = %s,
# #                 geocode_rating = %s,
# #                 geocode_source = 'tiger',
# #                 rating_threshold_used = %s,
# #                 normalized_input = %s,
# #                 tiger_data_year = %s,
# #                 tiger_address_num = %s,
# #                 tiger_predir = %s,
# #                 tiger_street_name = %s,
# #                 tiger_street_type = %s,
# #                 tiger_postdir = %s,
# #                 tiger_city = %s,
# #                 tiger_state = %s,
# #                 tiger_zip = %s,
# #                 geocoded_at = now(),
# #                 geocode_attempted_at = now(),
# #                 geocode_fail_reason = NULL
# #             WHERE address_hash = %s
# #             """,
# #             [
# #                 geocode_result["latitude"],
# #                 geocode_result["longitude"],
# #                 geocode_result["geom"],
# #                 geocode_result["rating"],
# #                 self.rating_threshold,
# #                 geocode_input,
# #                 self.tiger_data_year,
# #                 parts.get("tiger_address_num"),
# #                 parts.get("tiger_predir"),
# #                 parts.get("tiger_street_name"),
# #                 parts.get("tiger_street_type"),
# #                 parts.get("tiger_postdir"),
# #                 parts.get("tiger_city"),
# #                 parts.get("tiger_state"),
# #                 parts.get("tiger_zip"),
# #                 address_hash,
# #             ],
# #         )

# #     def _update_failure(self, address_hash: str, reason: str) -> None:
# #         """Record a failed geocoding attempt.

# #         Args:
# #             address_hash: The row's address_hash PK.
# #             reason: A failure reason string. Strings starting with 'error:'
# #                 are treated as transient (will be retried). All others are
# #                 treated as permanent.
# #         """
# #         self.engine.execute(
# #             f"""
# #             UPDATE {self.schema_name}.{self.table_name}
# #             SET geocode_attempted_at = now(),
# #                 geocode_fail_reason = %s
# #             WHERE address_hash = %s
# #             """,
# #             [reason, address_hash],
# #         )

# #     def process_row(self, row: dict) -> str:
# #         """Normalize and geocode a single row, updating the cache accordingly.

# #         Args:
# #             row: A dict from get_pending_batch().

# #         Returns:
# #             One of: 'success', 'permanent_failure', 'transient_failure'.
# #         """
# #         address_hash = row["address_hash"]
# #         geocode_input = self._build_geocode_input(row)

# #         normalized_parts = self.normalize(geocode_input)

# #         try:
# #             result = self.geocode(geocode_input)
# #         except Exception as e:
# #             self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
# #             return "transient_failure"

# #         if result is None:
# #             self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
# #             logger.info("No result for %s (%s)", address_hash, geocode_input)
# #             return "permanent_failure"

# #         if result["rating"] > self.rating_threshold:
# #             reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
# #             self._update_failure(address_hash, reason)
# #             logger.info(
# #                 "Below threshold for %s (%s): rating %d > %d",
# #                 address_hash,
# #                 geocode_input,
# #                 result["rating"],
# #                 self.rating_threshold,
# #             )
# #             return "permanent_failure"

# #         self._update_success(address_hash, result, normalized_parts, geocode_input)
# #         logger.debug(
# #             "Geocoded %s (%s): rating %d",
# #             address_hash,
# #             geocode_input,
# #             result["rating"],
# #         )
# #         return "success"

# #     def process_all(self) -> dict:
# #         """Process all pending addresses in batches.

# #         Loops until no more pending rows are returned. Each batch is fetched,
# #         processed row-by-row, and results are written immediately so progress
# #         is not lost if the process is interrupted.

# #         Returns:
# #             A summary dict with counts of successes, permanent failures,
# #             transient failures, and total processed.
# #         """
# #         counts = {
# #             "success": 0,
# #             "permanent_failure": 0,
# #             "transient_failure": 0,
# #         }

# #         while True:
# #             batch = self.get_pending_batch()
# #             if not batch:
# #                 break

# #             logger.info("Processing batch of %d addresses", len(batch))

# #             for row in batch:
# #                 outcome = self.process_row(row)
# #                 counts[outcome] += 1

# #         total = sum(counts.values())
# #         logger.info(
# #             "Geocoding complete: %d total, %d successes, %d permanent failures, %d transient failures",
# #             total,
# #             counts["success"],
# #             counts["permanent_failure"],
# #             counts["transient_failure"],
# #         )

# #         return {"total": total, **counts}

# #     # def normalize(self, address_string: str) -> dict | None:
# #     #     """Parse an address string using TIGER's normalize_address().

# #     #     Args:
# #     #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

# #     #     Returns:
# #     #         A dict with TIGER's parsed address parts, or None if normalization fails.
# #     #     """
# #     #     try:
# #     #         result = self.engine.query(
# #     #             """
# #     #             select
# #     #                 (n).address::text     AS tiger_address_num,
# #     #                 (n).predirabbrev      AS tiger_predir,
# #     #                 (n).streetname        AS tiger_street_name,
# #     #                 (n).streettypeabbrev  AS tiger_street_type,
# #     #                 (n).postdirabbrev     AS tiger_postdir,
# #     #                 (n).location          AS tiger_city,
# #     #                 (n).stateabbrev       AS tiger_state,
# #     #                 (n).zip               AS tiger_zip
# #     #             from normalize_address(%s) AS n
# #     #             """,
# #     #             [address_string],
# #     #         )
# #     #     except Exception as e:
# #     #         logger.warning("normalize_address() failed for %r: %s", address_string, e)
# #     #         return None

# #     #     if not result:
# #     #         return None

# #     #     return dict(result[0])

# #     # def geocode(self, address_string: str) -> dict | None:
# #     #     """Geocode an address string using TIGER's geocode().

# #     #     Args:
# #     #         address_string: A full address string (e.g. "100 N BROADWAY, CHICAGO, IL").

# #     #     Returns:
# #     #         A dict with keys: rating, latitude, longitude, geom. Or None if
# #     #         geocode() returns no results.
# #     #     """
# #     #     try:
# #     #         result = self.engine.query(
# #     #             """
# #     #             select
# #     #                 g.rating,
# #     #                 ST_Y(g.geomout)  AS latitude,
# #     #                 ST_X(g.geomout)  AS longitude,
# #     #                 g.geomout        AS geom
# #     #             from geocode(%s, 1) AS g
# #     #             """,
# #     #             [address_string],
# #     #         )
# #     #     except Exception as e:
# #     #         logger.warning("geocode() failed for %r: %s", address_string, e)
# #     #         raise

# #     #     if not result:
# #     #         return None

# #     #     row = result[0]
# #     #     return {
# #     #         "rating": row["rating"],
# #     #         "latitude": row["latitude"],
# #     #         "longitude": row["longitude"],
# #     #         "geom": row["geom"],
# #     #     }

# #     # def get_pending_batch(self) -> list[dict]:
# #     #     """Fetch the next batch of addresses that need geocoding.

# #     #     Returns rows where:
# #     #     - latitude is NULL (not yet geocoded), AND
# #     #     - either never attempted, or the last attempt was a transient error.

# #     #     Returns:
# #     #         A list of row dicts with address_hash, raw_address, street,
# #     #         city, state, zip.
# #     #     """
# #     #     return self.engine.query(
# #     #         f"""
# #     #         select address_hash, raw_address, street, city, state, zip
# #     #         from {self.schema_name}.{self.table_name}
# #     #         where latitude is null
# #     #           and (
# #     #               geocode_fail_reason is null
# #     #               OR geocode_fail_reason like '{self.TRANSIENT_PREFIX}:%%'
# #     #           )
# #     #         limit %s
# #     #         """,
# #     #         [self.batch_size],
# #     #     )

# #     # def _build_geocode_input(self, row: dict) -> str:
# #     #     """Build the address string to pass to geocode().

# #     #     Uses structured parts if available, otherwise falls back to raw_address.

# #     #     Args:
# #     #         row: A dict with keys: street, city, state, zip, raw_address.

# #     #     Returns:
# #     #         An address string suitable for geocode().
# #     #     """
# #     #     if row.get("street") and row.get("state"):
# #     #         parts = [row["street"]]
# #     #         if row.get("city"):
# #     #             parts.append(row["city"])
# #     #         parts.append(row["state"])
# #     #         if row.get("zip"):
# #     #             parts[-1] = f"{row['state']} {row['zip']}"
# #     #         return ", ".join(parts)
# #     #     return row["raw_address"]

# #     # def _update_success(
# #     #     self,
# #     #     address_hash: str,
# #     #     geocode_result: dict,
# #     #     normalized_parts: dict | None,
# #     #     geocode_input: str,
# #     # ) -> None:
# #     #     """Write a successful geocoding result to the cache.

# #     #     Args:
# #     #         address_hash: The row's address_hash PK.
# #     #         geocode_result: Dict with rating, latitude, longitude, geom.
# #     #         normalized_parts: Dict from normalize(), or None.
# #     #         geocode_input: The exact string passed to geocode().
# #     #     """
# #     #     parts = normalized_parts or {}
# #     #     self.engine.execute(
# #     #         f"""
# #     #         update {self.schema_name}.{self.table_name}
# #     #         set latitude = %s,
# #     #             longitude = %s,
# #     #             geom = %s,
# #     #             geocode_rating = %s,
# #     #             geocode_source = 'tiger',
# #     #             rating_threshold_used = %s,
# #     #             normalized_input = %s,
# #     #             tiger_data_year = %s,
# #     #             tiger_address_num = %s,
# #     #             tiger_predir = %s,
# #     #             tiger_street_name = %s,
# #     #             tiger_street_type = %s,
# #     #             tiger_postdir = %s,
# #     #             tiger_city = %s,
# #     #             tiger_state = %s,
# #     #             tiger_zip = %s,
# #     #             geocoded_at = now(),
# #     #             geocode_attempted_at = now(),
# #     #             geocode_fail_reason = null
# #     #         where address_hash = %s
# #     #         """,
# #     #         [
# #     #             geocode_result["latitude"],
# #     #             geocode_result["longitude"],
# #     #             geocode_result["geom"],
# #     #             geocode_result["rating"],
# #     #             self.rating_threshold,
# #     #             geocode_input,
# #     #             self.tiger_data_year,
# #     #             parts.get("tiger_address_num"),
# #     #             parts.get("tiger_predir"),
# #     #             parts.get("tiger_street_name"),
# #     #             parts.get("tiger_street_type"),
# #     #             parts.get("tiger_postdir"),
# #     #             parts.get("tiger_city"),
# #     #             parts.get("tiger_state"),
# #     #             parts.get("tiger_zip"),
# #     #             address_hash,
# #     #         ],
# #     #     )

# #     # def _update_failure(self, address_hash: str, reason: str) -> None:
# #     #     """Record a failed geocoding attempt.

# #     #     Args:
# #     #         address_hash: The row's address_hash PK.
# #     #         reason: A failure reason string. Strings starting with 'error:'
# #     #             are treated as transient (will be retried). All others are
# #     #             treated as permanent.
# #     #     """
# #     #     self.engine.execute(
# #     #         f"""
# #     #         update {self.schema_name}.{self.table_name}
# #     #         set geocode_attempted_at = now(),
# #     #             geocode_fail_reason = %s
# #     #         where address_hash = %s
# #     #         """,
# #     #         [reason, address_hash],
# #     #     )

# #     # def _process_row(self, row: dict) -> None:
# #     #     """Normalize and geocode a single row, updating the cache accordingly.

# #     #     Args:
# #     #         row: A dict from get_pending_batch().
# #     #     """
# #     #     address_hash = row["address_hash"]
# #     #     geocode_input = self._build_geocode_input(row)

# #     #     # Normalize
# #     #     normalized_parts = self.normalize(geocode_input)

# #     #     # Geocode
# #     #     try:
# #     #         result = self.geocode(geocode_input)
# #     #     except Exception as e:
# #     #         self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
# #     #         return

# #     #     if result is None:
# #     #         self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
# #     #         logger.info("No result for %s (%s)", address_hash, geocode_input)
# #     #         return

# #     #     if result["rating"] > self.rating_threshold:
# #     #         reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
# #     #         self._update_failure(address_hash, reason)
# #     #         logger.info(
# #     #             "Below threshold for %s (%s): rating %d > %d",
# #     #             address_hash,
# #     #             geocode_input,
# #     #             result["rating"],
# #     #             self.rating_threshold,
# #     #         )
# #     #         return

# #     #     self._update_success(address_hash, result, normalized_parts, geocode_input)
# #     #     logger.debug(
# #     #         "Geocoded %s (%s): rating %d",
# #     #         address_hash,
# #     #         geocode_input,
# #     #         result["rating"],
# #     #     )

# #     # def process_all(self) -> dict:
# #     #     """Process all pending addresses in batches.

# #     #     Loops until no more pending rows are returned. Each batch is fetched,
# #     #     processed row-by-row, and results are written immediately so progress
# #     #     is not lost if the process is interrupted.

# #     #     Returns:
# #     #         A summary dict with counts of successes, permanent failures,
# #     #         transient failures, and total processed.
# #     #     """
# #     #     total = 0
# #     #     successes = 0
# #     #     permanent_failures = 0
# #     #     transient_failures = 0

# #     #     while True:
# #     #         batch = self.get_pending_batch()
# #     #         if not batch:
# #     #             break

# #     #         logger.info("Processing batch of %d addresses", len(batch))

# #     #         for row in batch:
# #     #             address_hash = row["address_hash"]
# #     #             geocode_input = self._build_geocode_input(row)
# #     #             normalized_parts = self.normalize(geocode_input)

# #     #             try:
# #     #                 result = self.geocode(geocode_input)
# #     #             except Exception as e:
# #     #                 self._update_failure(address_hash, f"{self.TRANSIENT_PREFIX}:{e}")
# #     #                 transient_failures += 1
# #     #                 total += 1
# #     #                 continue

# #     #             if result is None:
# #     #                 self._update_failure(address_hash, self.PERMANENT_PREFIX_NO_RESULT)
# #     #                 permanent_failures += 1
# #     #                 total += 1
# #     #                 logger.info("No result for %s (%s)", address_hash, geocode_input)
# #     #                 continue

# #     #             if result["rating"] > self.rating_threshold:
# #     #                 reason = f"{self.PERMANENT_PREFIX_BELOW_THRESHOLD}:rating_{result['rating']}"
# #     #                 self._update_failure(address_hash, reason)
# #     #                 permanent_failures += 1
# #     #                 total += 1
# #     #                 logger.info(
# #     #                     "Below threshold for %s (%s): rating %d > %d",
# #     #                     address_hash,
# #     #                     geocode_input,
# #     #                     result["rating"],
# #     #                     self.rating_threshold,
# #     #                 )
# #     #                 continue

# #     #             self._update_success(
# #     #                 address_hash, result, normalized_parts, geocode_input
# #     #             )
# #     #             successes += 1
# #     #             total += 1
# #     #             logger.debug(
# #     #                 "Geocoded %s (%s): rating %d",
# #     #                 address_hash,
# #     #                 geocode_input,
# #     #                 result["rating"],
# #     #             )

# #     #     logger.info(
# #     #         "Geocoding complete: %d total, %d successes, %d permanent failures, %d transient failures",
# #     #         total,
# #     #         successes,
# #     #         permanent_failures,
# #     #         transient_failures,
# #     #     )

# #     #     return {
# #     #         "total": total,
# #     #         "successes": successes,
# #     #         "permanent_failures": permanent_failures,
# #     #         "transient_failures": transient_failures,
# #     #     }
