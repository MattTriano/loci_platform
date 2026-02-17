from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collectors.ingestion_tracker import IngestionRun, IngestionTracker
from collectors.socrata import (
    IncrementalConfig,
    SocrataClient,
    SocrataCollector,
    SocrataTableMetadata,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_metadata_mock(domain: str = "data.example.org"):
    """Create a mock SocrataTableMetadata."""
    meta = MagicMock(spec=SocrataTableMetadata)
    meta.table_id = "abcd-1234"
    meta.domain = domain
    return meta


def _make_batch(*rows: dict) -> list[dict]:
    """Convenience: build a batch list from row dicts."""
    return list(rows)


class FakeStager:
    """
    A lightweight fake for StagedIngest that tracks write_batch calls
    and lets tests set rows_staged / rows_merged.
    """

    def __init__(self):
        self.batches: list[list[dict]] = []
        self.rows_staged = 0
        self.rows_merged = 0

    def write_batch(self, rows: list[dict]) -> int:
        self.batches.append(rows)
        self.rows_staged += len(rows)
        return len(rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Simulate merge: rows_merged = rows_staged unless overridden
        if self.rows_merged == 0:
            self.rows_merged = self.rows_staged
        return False


@pytest.fixture
def mock_engine():
    """A mock PostgresEngine with staged_ingest returning a FakeStager."""
    engine = MagicMock()
    engine.execute.return_value = None

    # Each call to staged_ingest returns a fresh FakeStager
    engine._stagers = []

    def make_stager(**kwargs):
        stager = FakeStager()
        engine._stagers.append(stager)
        return stager

    engine.staged_ingest.side_effect = make_stager
    return engine


@pytest.fixture
def tracker(mock_engine):
    return IngestionTracker(engine=mock_engine)


@pytest.fixture
def collector(mock_engine, tmp_path):
    """SocrataCollector with mocked engine and auto-created tracker."""
    return SocrataCollector(engine=mock_engine, page_size=100)


def _attach_mock_client(collector, pages: list[list[dict]]):
    """Inject a mock SocrataClient that returns the given pages."""
    mock_client = MagicMock(spec=SocrataClient)
    mock_client.paginate.return_value = iter(pages)
    collector._client = mock_client
    return mock_client


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

        # Check the params passed to engine.execute for the INSERT
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
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:  #  noqa F841
            pass  # no HWM set

        assert tracker.get_high_water_mark("socrata", "abcd-1234") is None

    def test_persist_handles_engine_error_gracefully(self, tracker, mock_engine):
        mock_engine.execute.side_effect = RuntimeError("db down")
        # Should not raise — just logs
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


# ---------------------------------------------------------------------------
# _extract_max_hwm tests
# ---------------------------------------------------------------------------


class TestExtractMaxHwm:
    def test_extracts_max_value(self):
        batch = _make_batch(
            {"ts": "2024-01-01", "socrata_id": "a"},
            {"ts": "2024-03-01", "socrata_id": "b"},
            {"ts": "2024-02-01", "socrata_id": "c"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-03-01"
        assert sid == "b"

    def test_skips_none_values(self):
        batch = _make_batch(
            {"ts": None, "socrata_id": "a"},
            {"ts": "2024-01-01", "socrata_id": "b"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-01-01"
        assert sid == "b"

    def test_returns_none_tuple_for_empty_batch(self):
        assert SocrataCollector._extract_max_hwm([], "ts") == (None, None)

    def test_returns_none_tuple_for_all_nulls(self):
        batch = _make_batch({"ts": None}, {"ts": None})
        assert SocrataCollector._extract_max_hwm(batch, "ts") == (None, None)

    def test_tiebreaks_on_socrata_id(self):
        batch = _make_batch(
            {"ts": "2024-03-01", "socrata_id": "aaa"},
            {"ts": "2024-03-01", "socrata_id": "zzz"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-03-01"
        assert sid == "zzz"


# ---------------------------------------------------------------------------
# SocrataCollector.incremental_update tests
# ---------------------------------------------------------------------------


class TestIncrementalUpdate:
    CONFIG = IncrementalConfig(
        incremental_column="updated_on",
        conflict_key=["id"],
    )

    def test_basic_multi_page_ingest(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()

        page1 = _make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        page2 = _make_batch(
            {"id": "3", "updated_on": "2024-01-03", "val": "c"},
        )
        _attach_mock_client(collector, [page1, page2])

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG
        )

        # Test outcomes, not implementation details
        assert total == 3

        # Verify the right table was targeted
        call_kwargs = mock_engine.staged_ingest.call_args.kwargs
        assert call_kwargs["target_table"] == "test"
        assert call_kwargs["target_schema"] == "raw_data"
        assert call_kwargs["conflict_column"] == ["id"]

        # Verify both pages were written
        stager = mock_engine._stagers[0]
        assert len(stager.batches) == 2
        assert len(stager.batches[0]) == 2
        assert len(stager.batches[1]) == 1

    def test_hwm_override_appears_in_where_clause(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()
        mock_client = _attach_mock_client(collector, [])

        collector.incremental_update(
            "abcd-1234",
            "test",
            "raw_data",
            self.CONFIG,
            high_water_mark_override="2024-06-01",
        )

        where = mock_client.paginate.call_args[1]["where"]
        assert "2024-06-01" in where

    def test_static_where_combined_with_hwm(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()
        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
            where="status = 'active'",
        )
        mock_client = _attach_mock_client(collector, [])

        collector.incremental_update(
            "abcd-1234",
            "test",
            "raw_data",
            config,
            high_water_mark_override="2024-01-01",
        )

        where = mock_client.paginate.call_args[1]["where"]
        assert "status = 'active'" in where
        assert "2024-01-01" in where

    def test_hwm_tracked_after_successful_run(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()

        batch = _make_batch(
            {"id": "1", "updated_on": "2024-01-01"},
            {"id": "2", "updated_on": "2024-03-15"},
            {"id": "3", "updated_on": "2024-02-10"},
        )
        _attach_mock_client(collector, [batch])

        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        run = collector.tracker.last_run
        assert run.status == "success"
        assert run.rows_ingested == 3
        assert run.rows_staged == 3
        assert run.rows_merged == 3
        assert run.high_water_mark is not None
        assert "2024-03-15" in run.high_water_mark

    def test_failure_propagates_and_is_tracked(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()
        mock_client = MagicMock(spec=SocrataClient)
        mock_client.paginate.side_effect = RuntimeError("API down")
        collector._client = mock_client

        with pytest.raises(RuntimeError, match="API down"):
            collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        run = collector.tracker.last_run
        assert run.status == "failed"
        assert "API down" in run.error

    def test_empty_result_returns_zero(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()
        _attach_mock_client(collector, [])

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG
        )
        assert total == 0

    def test_system_fields_renamed_in_batches(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()

        batch = _make_batch(
            {":id": "row1", ":updated_at": "2024-01-01T00:00:00", "val": "x"},
        )
        _attach_mock_client(collector, [batch])

        config = IncrementalConfig(
            incremental_column=":updated_at",
            conflict_key=["socrata_id"],
        )
        collector.incremental_update("abcd-1234", "test", "raw_data", config)

        stager = mock_engine._stagers[0]
        assert len(stager.batches) == 1
        row = stager.batches[0][0]
        assert "socrata_id" in row
        assert "socrata_updated_at" in row
        assert ":id" not in row
        assert ":updated_at" not in row

    def test_keyset_hwm_with_pipe_id(self, collector, mock_engine):
        """HWM stored as 'value|id' should produce keyset pagination."""
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()

        # Simulate a prior HWM
        collector.tracker._hwm_cache[("socrata", "abcd-1234")] = "2024-06-01|row99"

        mock_client = _attach_mock_client(collector, [])
        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        where = mock_client.paginate.call_args[1]["where"]
        assert "2024-06-01" in where
        assert "row99" in where


# ---------------------------------------------------------------------------
# SocrataCollector.full_refresh_via_api tests
# ---------------------------------------------------------------------------


class TestFullRefreshViaApi:
    def test_delegates_to_incremental_update(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = _make_metadata_mock()
        _attach_mock_client(collector, [])

        total = collector.full_refresh_via_api("abcd-1234", "test", "raw_data")

        assert total == 0
        collector._client.paginate.assert_called_once()


# ---------------------------------------------------------------------------
# SocrataCollector.preview tests
# ---------------------------------------------------------------------------


class TestPreview:
    def test_delegates_to_client_query(self, collector):
        collector._metadata_cache["wxyz-5678"] = _make_metadata_mock()

        mock_client = MagicMock(spec=SocrataClient)
        mock_client.query.return_value = [{"col": "val"}]
        collector._client = mock_client

        result = collector.preview("wxyz-5678", limit=3, columns=["col"])

        assert result == [{"col": "val"}]
        mock_client.query.assert_called_once_with(
            domain="data.example.org",
            dataset_id="wxyz-5678",
            select="col",
            limit=3,
            include_system_fields=True,
        )


# ---------------------------------------------------------------------------
# SocrataClient tests
# ---------------------------------------------------------------------------


class TestSocrataClient:
    @patch("collectors.socrata.requests.Session")
    def test_paginate_single_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page_data = [{"id": "1"}, {"id": "2"}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = page_data
        mock_session.get.return_value = mock_resp

        client = SocrataClient(page_size=100)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 1
        assert pages[0] == page_data

    @patch("collectors.socrata.requests.Session")
    def test_paginate_stops_after_partial_page(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        page1 = [{"id": "1"}, {"id": "2"}]
        page2 = [{"id": "3"}]  # partial → last page
        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = page1
        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = page2
        mock_session.get.side_effect = [mock_resp1, mock_resp2]

        client = SocrataClient(page_size=2)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 2
        assert len(pages[0]) == 2
        assert len(pages[1]) == 1

    def test_query_builds_soda_params(self):
        client = SocrataClient()
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"a": 1}]
        mock_resp.headers = {}
        mock_session.get.return_value = mock_resp
        client._session = mock_session

        result = client.query(
            "example.com",
            "abcd-1234",
            select="col1, col2",
            where="col1 > 5",
            order="col1 DESC",
            limit=10,
        )

        params = mock_session.get.call_args[1]["params"]
        assert params["$select"] == "col1, col2"
        assert params["$where"] == "col1 > 5"
        assert params["$order"] == "col1 DESC"
        assert params["$limit"] == "10"
        assert result == [{"a": 1}]


# ---------------------------------------------------------------------------
# Metadata cache test
# ---------------------------------------------------------------------------


class TestMetadataCache:
    def test_caches_metadata_instance(self, collector):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        assert collector._get_metadata("abcd-1234") is meta
        assert collector._get_metadata("abcd-1234") is meta  # same object
