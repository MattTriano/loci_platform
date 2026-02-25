from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from loci.collectors.socrata.collector import SocrataCollector
from loci.tracking.ingestion_tracker import IngestionRun, IngestionTracker

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    engine = MagicMock()
    engine.execute.return_value = None
    engine.query.return_value = pd.DataFrame({"column_name": []})
    return engine


@pytest.fixture
def tracker(mock_engine):
    return IngestionTracker(engine=mock_engine)


# ---------------------------------------------------------------------------
# IngestionTracker tests
# ---------------------------------------------------------------------------


class TestIngestionTracker:
    def test_auto_creates_tracker_when_none_provided(self, mock_engine):
        c = SocrataCollector(engine=mock_engine)
        assert isinstance(c.tracker, IngestionTracker)
        assert c.tracker.engine is mock_engine

    def test_uses_provided_tracker(self, mock_engine):
        custom_tracker = IngestionTracker()
        c = SocrataCollector(engine=mock_engine, tracker=custom_tracker)
        assert c.tracker is custom_tracker

    def test_track_creates_successful_run(self, tracker):
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:
            run.rows_ingested = 42
        assert run.status == "success"
        assert run.rows_ingested == 42
        assert tracker.last_run is run

    def test_track_records_failure(self, tracker):
        with pytest.raises(ValueError, match="boom"):
            with tracker.track("socrata", "abcd-1234", "raw.table") as run:
                raise ValueError("boom")
        assert run.status == "failed"
        assert "boom" in run.error

    def test_get_hwm_returns_none_without_engine(self):
        tracker = IngestionTracker(engine=None)
        assert tracker.get_high_water_mark("socrata", "abcd-1234") is None

    def test_runs_accumulate(self, tracker):
        with tracker.track("s", "d1", "t1"):
            pass
        with tracker.track("s", "d2", "t2"):
            pass
        assert len(tracker.runs) == 2

    def test_persist_run_records_rows_staged_and_merged(self, tracker, mock_engine):
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:
            run.rows_staged = 100
            run.rows_merged = 95
            run.rows_ingested = 95

        insert_call = mock_engine.execute.call_args
        params = insert_call[0][1]
        assert params["rows_staged"] == 100
        assert params["rows_merged"] == 95
        assert params["rows_ingested"] == 95

    def test_persist_run_caches_hwm(self, tracker, mock_engine):
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:
            run.high_water_mark = "2024-06-01|abc"

        cached = tracker.get_high_water_mark("socrata", "abcd-1234")
        assert cached == "2024-06-01|abc"

    def test_persist_run_does_not_cache_null_hwm(self, tracker, mock_engine):
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:  # noqa: F841
            pass

        assert tracker.get_high_water_mark("socrata", "abcd-1234") is None

    def test_persist_handles_engine_error_gracefully(self, tracker, mock_engine):
        mock_engine.execute.side_effect = RuntimeError("db down")
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:
            run.rows_ingested = 10

        assert run.status == "success"

    def test_last_run_is_none_when_empty(self):
        tracker = IngestionTracker(engine=None)
        assert tracker.last_run is None


# ---------------------------------------------------------------------------
# IngestionRun tests
# ---------------------------------------------------------------------------


class TestIngestionRun:
    def test_context_manager_sets_timestamps(self):
        run = IngestionRun(source="s", dataset_id="d", target_table="t")
        with run:
            assert run.started_at is not None
        assert run.completed_at is not None
        assert run.completed_at >= run.started_at

    def test_context_manager_captures_error(self):
        run = IngestionRun(source="s", dataset_id="d", target_table="t")
        with pytest.raises(RuntimeError, match="test error"):
            with run:
                raise RuntimeError("test error")
        assert run.status == "failed"
        assert "test error" in run.error

    def test_default_values(self):
        run = IngestionRun(source="s", dataset_id="d", target_table="t")
        assert run.rows_ingested == 0
        assert run.rows_staged == 0
        assert run.rows_merged == 0
        assert run.status == "running"
        assert run.high_water_mark is None
        assert run.error is None
