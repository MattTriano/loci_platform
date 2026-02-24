from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class IngestionRun:
    """Tracks a single ingestion run's state."""

    source: str
    dataset_id: str
    target_table: str
    metadata: dict[str, Any] = field(default_factory=dict)
    rows_ingested: int = 0
    rows_staged: int = 0
    rows_merged: int = 0
    high_water_mark: str | None = None
    status: str = "running"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None

    def __enter__(self) -> IngestionRun:
        self.started_at = datetime.now(UTC)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.completed_at = datetime.now(UTC)
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

    @contextmanager
    def track(
        self,
        source: str,
        dataset_id: str,
        target_table: str,
        metadata: dict[str, Any] | None = None,
    ) -> IngestionRun:
        """Create, yield, and persist an IngestionRun."""
        run = IngestionRun(
            source=source,
            dataset_id=dataset_id,
            target_table=target_table,
            metadata=metadata or {},
        )
        self._runs.append(run)
        with run:
            yield run
        self._persist_run(run)

    def _persist_run(self, run: IngestionRun) -> None:
        if self.engine is None:
            return
        try:
            self.engine.execute(
                f"""
                insert into {self.TABLE_NAME}
                    (source, dataset_id, target_table, status,
                     rows_ingested, rows_staged, rows_merged,
                     high_water_mark, metadata,
                     started_at, completed_at, error_message)
                values
                    (%(source)s, %(dataset_id)s, %(target_table)s, %(status)s,
                     %(rows_ingested)s, %(rows_staged)s, %(rows_merged)s,
                     %(high_water_mark)s, %(metadata)s,
                     %(started_at)s, %(completed_at)s, %(error_message)s)
                """,
                {
                    "source": run.source,
                    "dataset_id": run.dataset_id,
                    "target_table": run.target_table,
                    "status": run.status,
                    "rows_ingested": run.rows_ingested,
                    "rows_staged": run.rows_staged,
                    "rows_merged": run.rows_merged,
                    "high_water_mark": run.high_water_mark,
                    "metadata": json.dumps(run.metadata),
                    "started_at": run.started_at,
                    "completed_at": run.completed_at,
                    "error_message": run.error,
                },
            )
            if run.high_water_mark:
                self._hwm_cache[(run.source, run.dataset_id)] = run.high_water_mark
        except Exception as e:
            self.logger.error("Failed to persist ingestion run: %s", e)

    def get_high_water_mark(self, source: str, dataset_id: str) -> str | None:
        """Look up the last successful high-water mark for a dataset."""
        cache_key = (source, dataset_id)
        if cache_key in self._hwm_cache:
            return self._hwm_cache[cache_key]

        if self.engine is None:
            return None

        try:
            df = self.engine.query(
                f"""
                select high_water_mark
                from {self.TABLE_NAME}
                where
                    source = %(source)s
                    and dataset_id = %(dataset_id)s
                    and status = 'success'
                    and high_water_mark is not null
                order by completed_at desc
                limit 1
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
    def last_run(self) -> IngestionRun | None:
        return self._runs[-1] if self._runs else None
