from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


logger = logging.getLogger(__name__)


@dataclass
class IngestionRun:
    """Tracks a single ingestion run's state."""

    source: str
    dataset_id: str
    target_table: str
    metadata: dict[str, Any] = field(default_factory=dict)
    rows_ingested: int = 0
    high_water_mark: Optional[str] = None
    status: str = "running"
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None

    def __enter__(self) -> "IngestionRun":
        self.started_at = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.finished_at = datetime.now(timezone.utc)
        if exc_type is not None:
            self.status = "failed"
            self.error = str(exc_val)
        else:
            self.status = "success"
        return False


class IngestionTracker:
    """
    Tracks ingestion runs and manages high-water marks.

    When backed by a PostgresEngine, persists run history and HWM to the
    database. When no engine is provided, operates in memory-only mode
    (useful for testing or preview workflows).
    """

    TABLE_NAME = "meta.ingest_log"

    def __init__(self, engine: Any | None = None) -> None:
        self.engine = engine
        self.logger = logging.getLogger("ingestion_tracker")
        self._runs: list[IngestionRun] = []
        self._hwm_cache: dict[tuple[str, str], str] = {}

    def track(
        self,
        source: str,
        dataset_id: str,
        target_table: str,
        metadata: dict[str, Any] | None = None,
    ) -> IngestionRun:
        """Create and return an IngestionRun context manager."""
        run = IngestionRun(
            source=source,
            dataset_id=dataset_id,
            target_table=target_table,
            metadata=metadata or {},
        )
        self._runs.append(run)
        return run

    def get_high_water_mark(self, source: str, dataset_id: str) -> Optional[str]:
        """Look up the last successful high-water mark for a dataset."""
        cache_key = (source, dataset_id)
        if cache_key in self._hwm_cache:
            return self._hwm_cache[cache_key]

        if self.engine is None:
            return None

        try:
            df = self.engine.query(
                f"""
                SELECT high_water_mark
                FROM {self.TABLE_NAME}
                WHERE source = %(source)s
                  AND dataset_id = %(dataset_id)s
                  AND status = 'success'
                  AND high_water_mark IS NOT NULL
                ORDER BY finished_at DESC
                LIMIT 1
                """,
                {"source": source, "dataset_id": dataset_id},
            )
            if not df.empty:
                hwm = str(df.iloc[0]["high_water_mark"])
                self._hwm_cache[cache_key] = hwm
                return hwm
        except Exception as e:
            self.logger.warning("Failed to retrieve HWM: %s", e)

        return None

    @property
    def runs(self) -> list[IngestionRun]:
        return list(self._runs)

    @property
    def last_run(self) -> Optional[IngestionRun]:
        return self._runs[-1] if self._runs else None


# @dataclass
# class IngestionRun:
#     """Captures the state of a single ingestion run in progress."""

#     source: str
#     dataset: str
#     target_table: str
#     started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
#     rows_ingested: int = 0
#     high_water_mark: Optional[str] = None
#     status: str = "running"
#     error_message: Optional[str] = None
#     metadata: Optional[dict[str, Any]] = None

# class IngestionTracker:
#     """
#     Logs ingestion runs to meta.ingest_log and provides high-water mark lookups.

#     Designed to be injected into any collector class. Keeps no collection logic —
#     it only knows about tracking, so collectors stay focused on extraction.

#     Usage:
#         tracker = IngestionTracker(engine)

#         with tracker.track("socrata", "ijzp-q8t2", "raw_data.crimes") as run:
#             # ... do work, updating run.rows_ingested as you go ...
#             run.rows_ingested += batch_size
#             run.high_water_mark = last_seen_value

#         # On normal exit: logs status='success'
#         # On exception:   logs status='failed' with error details, then re-raises
#     """

#     LOG_TABLE = "meta.ingest_log"

#     def __init__(self, engine: Any) -> None:
#         self.engine = engine
#         self.logger = logging.getLogger("ingestion_tracker")

#     @contextmanager
#     def track(
#         self,
#         source: str,
#         dataset: str,
#         target_table: str,
#         metadata: dict[str, Any] | None = None,
#     ) -> Iterator[IngestionRun]:
#         """
#         Context manager that wraps an ingestion run.

#         Yields an IngestionRun whose fields the caller updates during processing.
#         On exit, writes a row to meta.ingest_log with final state.
#         """
#         run = IngestionRun(
#             source=source,
#             dataset=dataset,
#             target_table=target_table,
#             metadata=metadata,
#         )
#         self.logger.info(
#             "Starting ingestion run: source=%s dataset=%s target=%s",
#             source,
#             dataset,
#             target_table,
#         )

#         try:
#             yield run
#             run.status = "success"
#         except Exception as exc:
#             run.status = "failed"
#             run.error_message = f"{type(exc).__name__}: {exc}"
#             self.logger.error(
#                 "Ingestion run failed for %s/%s: %s",
#                 source,
#                 dataset,
#                 run.error_message,
#             )
#             raise
#         finally:
#             self._log_run(run)

#     def get_high_water_mark(
#         self,
#         source: str,
#         dataset: str,
#     ) -> Optional[str]:
#         """
#         Return the most recent successful high_water_mark for a source/dataset pair,
#         or None if no successful run exists.
#         """
#         sql = f"""
#             SELECT high_water_mark
#             FROM {self.LOG_TABLE}
#             WHERE source = %(source)s
#               AND dataset = %(dataset)s
#               AND status = 'success'
#               AND high_water_mark IS NOT NULL
#             ORDER BY completed_at DESC
#             LIMIT 1
#         """
#         # engine.query returns a DataFrame
#         df = self.engine.query(sql, params={"source": source, "dataset": dataset})
#         if df.empty:
#             return None
#         return df.iloc[0]["high_water_mark"]

#     def get_run_history(
#         self,
#         source: str | None = None,
#         dataset: str | None = None,
#         limit: int = 20,
#     ) -> Any:
#         """Return recent ingestion runs as a DataFrame, optionally filtered."""
#         conditions = ["1=1"]
#         params: dict[str, Any] = {"limit": limit}

#         if source:
#             conditions.append("source = %(source)s")
#             params["source"] = source
#         if dataset:
#             conditions.append("dataset = %(dataset)s")
#             params["dataset"] = dataset

#         where = " AND ".join(conditions)
#         sql = f"""
#             SELECT id, source, dataset, target_table, rows_ingested,
#                    high_water_mark, started_at, completed_at, status, error_message
#             FROM {self.LOG_TABLE}
#             WHERE {where}
#             ORDER BY completed_at DESC
#             LIMIT %(limit)s
#         """
#         return self.engine.query(sql, params=params)

#     def _log_run(self, run: IngestionRun) -> None:
#         """Write a completed run to the ingest_log table."""
#         completed_at = datetime.now(timezone.utc)
#         row = {
#             "source": run.source,
#             "dataset": run.dataset,
#             "target_table": run.target_table,
#             "rows_ingested": run.rows_ingested,
#             "high_water_mark": run.high_water_mark,
#             "started_at": run.started_at.isoformat(),
#             "completed_at": completed_at.isoformat(),
#             "status": run.status,
#             "error_message": run.error_message,
#         }
#         if run.metadata:
#             import json
#             row["metadata"] = json.dumps(run.metadata)

#         self.engine.ingest_batch([row], self.LOG_TABLE)
#         self.logger.info(
#             "Logged run: %s/%s → %s | %s | %d rows | hwm=%s",
#             run.source,
#             run.dataset,
#             run.target_table,
#             run.status,
#             run.rows_ingested,
#             run.high_water_mark,
#         )
