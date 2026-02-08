"""Tests for the refactored SocrataCollector."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
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


@pytest.fixture
def mock_engine():
    """A mock PostgresEngine with common methods stubbed."""
    engine = MagicMock()
    engine.ingest_batch.return_value = 5
    engine.ingest_csv.return_value = 10
    engine.execute.return_value = None
    engine.transaction.return_value.__enter__ = MagicMock()
    engine.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return engine


@pytest.fixture
def tracker(mock_engine):
    return IngestionTracker(engine=mock_engine)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def _make_metadata_mock(is_geospatial: bool = False, domain: str = "data.example.org"):
    """Create a mock SocrataTableMetadata."""
    meta = MagicMock(spec=SocrataTableMetadata)
    meta.table_id = "abcd-1234"
    meta.domain = domain
    meta.is_geospatial = is_geospatial
    meta.download_format = "GeoJSON" if is_geospatial else "csv"
    meta.data_download_url = (
        f"https://{domain}/api/geospatial/abcd-1234?method=export&format=GeoJSON"
        if is_geospatial
        else f"https://{domain}/api/views/abcd-1234/rows.csv?accessType=DOWNLOAD"
    )
    return meta


@pytest.fixture
def collector(mock_engine, tmp_dir):
    """SocrataCollector with mocked engine and auto-created tracker."""
    return SocrataCollector(
        engine=mock_engine,
        download_dir=tmp_dir,
        page_size=100,
    )


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

    def test_track_creates_run(self, tracker):
        with tracker.track("socrata", "abcd-1234", "raw.table") as run:
            run.rows_ingested = 42
        assert run.status == "success"
        assert run.rows_ingested == 42
        assert tracker.last_run is run

    def test_track_records_failure(self, tracker):
        with pytest.raises(ValueError):
            with tracker.track("socrata", "abcd-1234", "raw.table") as run:
                raise ValueError("boom")
        assert run.status == "failed"
        assert "boom" in run.error

    def test_get_hwm_returns_none_without_engine(self):
        tracker = IngestionTracker(engine=None)
        assert tracker.get_high_water_mark("socrata", "abcd-1234") is None

    def test_runs_list(self, tracker):
        with tracker.track("s", "d1", "t1"):
            pass
        with tracker.track("s", "d2", "t2"):
            pass
        assert len(tracker.runs) == 2


# ---------------------------------------------------------------------------
# IngestionRun tests
# ---------------------------------------------------------------------------


class TestIngestionRun:
    def test_context_manager_sets_timestamps(self):
        run = IngestionRun(source="s", dataset_id="d", target_table="t")
        with run:
            assert run.started_at is not None
        assert run.finished_at is not None
        assert run.finished_at >= run.started_at

    def test_context_manager_captures_error(self):
        run = IngestionRun(source="s", dataset_id="d", target_table="t")
        with pytest.raises(RuntimeError):
            with run:
                raise RuntimeError("test error")
        assert run.status == "failed"
        assert "test error" in run.error


# ---------------------------------------------------------------------------
# SocrataCollector.full_refresh tests
# ---------------------------------------------------------------------------


class TestFullRefresh:
    def _write_csv(self, path: Path) -> Path:
        fp = path / "abcd-1234.csv"
        fp.write_text("col1,col2\na,1\nb,2\n")
        return fp

    def _write_geojson(self, path: Path) -> Path:
        fp = path / "abcd-1234.geojson"
        data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "A", "val": 1},
                    "geometry": {"type": "Point", "coordinates": [-87.6, 41.8]},
                },
                {
                    "type": "Feature",
                    "properties": {"name": "B", "val": 2},
                    "geometry": {"type": "Point", "coordinates": [-87.7, 41.9]},
                },
            ],
        }
        fp.write_text(json.dumps(data))
        return fp

    @patch("collectors.socrata.requests.get")
    def test_full_refresh_csv_append(self, mock_get, collector, mock_engine, tmp_dir):
        # Prepare: mock metadata and download
        meta = _make_metadata_mock(is_geospatial=False)
        collector._metadata_cache["abcd-1234"] = meta

        csv_path = self._write_csv(tmp_dir)

        # Mock the download response
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [csv_path.read_bytes()]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        mock_engine.ingest_csv.return_value = 2

        rows = collector.full_refresh("abcd-1234", "raw_data.test", mode="append")
        assert rows == 2
        mock_engine.ingest_csv.assert_called_once()

    @patch("collectors.socrata.requests.get")
    def test_full_refresh_csv_replace(self, mock_get, collector, mock_engine, tmp_dir):
        meta = _make_metadata_mock(is_geospatial=False)
        collector._metadata_cache["abcd-1234"] = meta

        csv_path = self._write_csv(tmp_dir)
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [csv_path.read_bytes()]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        mock_engine.ingest_csv.return_value = 2

        rows = collector.full_refresh("abcd-1234", "raw_data.test", mode="replace")
        assert rows == 2
        mock_engine.execute.assert_called()  # TRUNCATE was called
        mock_engine.ingest_csv.assert_called_once()

    @patch("collectors.socrata.requests.get")
    def test_full_refresh_geospatial_uses_ingest_geojson(
        self, mock_get, collector, mock_engine, tmp_dir
    ):
        """Verify geospatial files are loaded via ingest_geojson."""
        meta = _make_metadata_mock(is_geospatial=True)
        collector._metadata_cache["abcd-1234"] = meta

        geojson_path = self._write_geojson(tmp_dir)
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [geojson_path.read_bytes()]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        mock_engine.ingest_geojson.return_value = (2, [])  # inserted, failed

        rows = collector.full_refresh(
            "abcd-1234",
            "raw_data.geo_test",
            mode="upsert",
            conflict_key=["name"],
        )

        assert rows == 2
        mock_engine.ingest_geojson.assert_called_once()

    @patch("collectors.socrata.requests.get")
    def test_full_refresh_geospatial_replace_truncates(
        self, mock_get, collector, mock_engine, tmp_dir
    ):
        meta = _make_metadata_mock(is_geospatial=True)
        collector._metadata_cache["abcd-1234"] = meta

        geojson_path = self._write_geojson(tmp_dir)
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [geojson_path.read_bytes()]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        mock_engine.ingest_geojson.return_value = (2, [])  # inserted, failed

        collector.full_refresh("abcd-1234", "raw_data.geo_test", mode="replace")

        mock_engine.execute.assert_called_with("TRUNCATE TABLE raw_data.geo_test")

    def test_full_refresh_records_tracker_run(self, collector, mock_engine, tmp_dir):
        meta = _make_metadata_mock(is_geospatial=False)
        collector._metadata_cache["abcd-1234"] = meta

        csv_path = tmp_dir / "abcd-1234.csv"
        csv_path.write_text("a,b\n1,2\n")

        with patch("collectors.socrata.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.iter_content.return_value = [csv_path.read_bytes()]
            mock_resp.raise_for_status.return_value = None
            mock_get.return_value = mock_resp
            mock_engine.ingest_csv.return_value = 1

            collector.full_refresh("abcd-1234", "raw_data.t", mode="append")

        run = collector.tracker.last_run
        assert run is not None
        assert run.status == "success"
        assert run.rows_ingested == 1
        assert run.metadata["mode"] == "append"


# ---------------------------------------------------------------------------
# SocrataCollector.incremental_update tests
# ---------------------------------------------------------------------------


class TestIncrementalUpdate:
    def test_incremental_update_basic(self, collector, mock_engine):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        # Mock paginate to return two pages
        page1 = [
            {"id": "1", "updated_on": "2024-01-01T00:00:00", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02T00:00:00", "val": "b"},
        ]
        page2 = [
            {"id": "3", "updated_on": "2024-01-03T00:00:00", "val": "c"},
        ]
        mock_client = MagicMock()
        mock_client.paginate.return_value = iter([page1, page2])
        collector._client = mock_client

        mock_engine.ingest_batch.return_value = 2  # first page
        # Use side_effect for different return values per call
        mock_engine.ingest_batch.side_effect = [2, 1]

        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
        )

        total = collector.incremental_update("abcd-1234", "raw_data.test", config)

        assert total == 3
        assert mock_engine.ingest_batch.call_count == 2

    def test_incremental_update_with_hwm_override(self, collector, mock_engine):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        mock_client = MagicMock()
        mock_client.paginate.return_value = iter([])
        collector._client = mock_client

        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
        )

        total = collector.incremental_update(
            "abcd-1234",
            "raw_data.test",
            config,
            high_water_mark_override="2024-06-01",
        )

        assert total == 0
        # Verify the where clause includes the HWM
        call_kwargs = mock_client.paginate.call_args[1]
        assert "2024-06-01" in call_kwargs["where"]

    def test_incremental_update_with_static_where(self, collector, mock_engine):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        mock_client = MagicMock()
        mock_client.paginate.return_value = iter([])
        collector._client = mock_client

        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
            where="status = 'active'",
        )

        collector.incremental_update(
            "abcd-1234",
            "raw_data.test",
            config,
            high_water_mark_override="2024-01-01",
        )

        call_kwargs = mock_client.paginate.call_args[1]
        assert "status = 'active'" in call_kwargs["where"]
        assert "2024-01-01" in call_kwargs["where"]

    def test_incremental_update_tracks_hwm(self, collector, mock_engine):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        batch = [
            {"id": "1", "updated_on": "2024-01-01"},
            {"id": "2", "updated_on": "2024-03-15"},
            {"id": "3", "updated_on": "2024-02-10"},
        ]
        mock_client = MagicMock()
        mock_client.paginate.return_value = iter([batch])
        collector._client = mock_client
        mock_engine.ingest_batch.return_value = 3

        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
        )

        collector.incremental_update("abcd-1234", "raw_data.test", config)

        run = collector.tracker.last_run
        assert run.high_water_mark == "2024-03-15"

    def test_incremental_update_records_failure(self, collector, mock_engine):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        mock_client = MagicMock()
        mock_client.paginate.side_effect = RuntimeError("API down")
        collector._client = mock_client

        config = IncrementalConfig(
            incremental_column="updated_on",
            conflict_key=["id"],
        )

        with pytest.raises(RuntimeError, match="API down"):
            collector.incremental_update("abcd-1234", "raw_data.test", config)

        run = collector.tracker.last_run
        assert run.status == "failed"


# ---------------------------------------------------------------------------
# _extract_max_hwm tests
# ---------------------------------------------------------------------------


class TestExtractMaxHwm:
    def test_extracts_max_string(self):
        batch = [
            {"ts": "2024-01-01"},
            {"ts": "2024-03-01"},
            {"ts": "2024-02-01"},
        ]
        assert SocrataCollector._extract_max_hwm(batch, "ts") == "2024-03-01"

    def test_skips_none_values(self):
        batch = [
            {"ts": None},
            {"ts": "2024-01-01"},
        ]
        assert SocrataCollector._extract_max_hwm(batch, "ts") == "2024-01-01"

    def test_returns_none_for_empty_batch(self):
        assert SocrataCollector._extract_max_hwm([], "ts") is None

    def test_returns_none_for_all_nulls(self):
        batch = [{"ts": None}, {"ts": None}]
        assert SocrataCollector._extract_max_hwm(batch, "ts") is None


# ---------------------------------------------------------------------------
# SocrataCollector.preview tests
# ---------------------------------------------------------------------------


class TestPreview:
    def test_preview_delegates_to_client(self, collector):
        meta = _make_metadata_mock()
        collector._metadata_cache["wxyz-5678"] = meta

        mock_client = MagicMock()
        mock_client.query.return_value = [{"col": "val"}]
        collector._client = mock_client

        result = collector.preview("wxyz-5678", limit=3, columns=["col"])
        assert result == [{"col": "val"}]
        mock_client.query.assert_called_once_with(
            domain="data.example.org",
            dataset_id="wxyz-5678",
            select="col",
            limit=3,
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
    def test_paginate_multiple_pages(self, MockSession):
        mock_session = MagicMock()
        MockSession.return_value = mock_session

        # Page 1 full (2 items = page_size), page 2 partial (1 item)
        page1 = [{"id": "1"}, {"id": "2"}]
        page2 = [{"id": "3"}]
        mock_resp1 = MagicMock()
        mock_resp1.json.return_value = page1
        mock_resp2 = MagicMock()
        mock_resp2.json.return_value = page2
        mock_session.get.side_effect = [mock_resp1, mock_resp2]

        client = SocrataClient(page_size=2)
        client._session = mock_session

        pages = list(client.paginate("example.com", "abcd-1234"))
        assert len(pages) == 2

    def test_query_builds_params(self):
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

        call_kwargs = mock_session.get.call_args
        params = call_kwargs[1]["params"]
        assert params["$select"] == "col1, col2"
        assert params["$where"] == "col1 > 5"
        assert params["$order"] == "col1 DESC"
        assert params["$limit"] == "10"
        assert result == [{"a": 1}]


# ---------------------------------------------------------------------------
# Metadata cache test
# ---------------------------------------------------------------------------


class TestMetadataCache:
    def test_caches_metadata(self, collector):
        meta = _make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        assert collector._get_metadata("abcd-1234") is meta
        # Second call returns same object
        assert collector._get_metadata("abcd-1234") is meta
