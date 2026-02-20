from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from collectors.config import IncrementalConfig
from collectors.exceptions import SchemaDriftError
from collectors.socrata.client import SocrataClient
from collectors.socrata.collector import SocrataCollector

from .conftest import (
    attach_mock_client,
    make_batch,
    make_metadata_mock,
    stub_table_columns,
)

# ---------------------------------------------------------------------------
# _extract_max_hwm tests
# ---------------------------------------------------------------------------


class TestExtractMaxHwm:
    def test_extracts_max_value(self):
        batch = make_batch(
            {"ts": "2024-01-01", "socrata_id": "a"},
            {"ts": "2024-03-01", "socrata_id": "b"},
            {"ts": "2024-02-01", "socrata_id": "c"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-03-01"
        assert sid == "b"

    def test_skips_none_values(self):
        batch = make_batch(
            {"ts": None, "socrata_id": "a"},
            {"ts": "2024-01-01", "socrata_id": "b"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-01-01"
        assert sid == "b"

    def test_returns_none_tuple_for_empty_batch(self):
        assert SocrataCollector._extract_max_hwm([], "ts") == (None, None)

    def test_returns_none_tuple_for_all_nulls(self):
        batch = make_batch({"ts": None}, {"ts": None})
        assert SocrataCollector._extract_max_hwm(batch, "ts") == (None, None)

    def test_tiebreaks_on_socrata_id(self):
        batch = make_batch(
            {"ts": "2024-03-01", "socrata_id": "aaa"},
            {"ts": "2024-03-01", "socrata_id": "zzz"},
        )
        value, sid = SocrataCollector._extract_max_hwm(batch, "ts")
        assert value == "2024-03-01"
        assert sid == "zzz"


# ---------------------------------------------------------------------------
# Preflight column check tests
# ---------------------------------------------------------------------------


class TestPreflightColumnCheck:
    """Tests for the schema drift detection that runs on the first batch."""

    def test_raises_on_unknown_source_columns(self, collector, mock_engine):
        """Columns in the source but not the target table cause SchemaDriftError."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()

        stub_table_columns(mock_engine, ["id", "socrata_id", "socrata_updated_at"])

        batch = make_batch(
            {
                "id": "1",
                "extra_col": "surprise",
                ":id": "r1",
                ":updated_at": "2024-01-01",
            },
        )
        attach_mock_client(collector, [batch])

        config = IncrementalConfig(
            incremental_column=":updated_at",
            entity_key=["id"],
        )
        with pytest.raises(SchemaDriftError, match="extra_col"):
            collector.incremental_update("abcd-1234", "test", "raw_data", config)

    def test_metadata_columns_are_ignored_in_drift_check(self, collector, mock_engine):
        """System/metadata columns shouldn't trigger schema drift."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()

        stub_table_columns(
            mock_engine,
            [
                "id",
                "val",
                "socrata_id",
                "socrata_updated_at",
                "socrata_created_at",
                "socrata_version",
                "ingested_at",
            ],
        )

        batch = make_batch(
            {"id": "1", "val": "a", ":id": "r1", ":updated_at": "2024-01-01"},
        )
        attach_mock_client(collector, [batch])

        config = IncrementalConfig(
            incremental_column=":updated_at",
            entity_key=["id"],
        )
        total = collector.incremental_update("abcd-1234", "test", "raw_data", config)
        assert total == 1


# ---------------------------------------------------------------------------
# SocrataCollector.incremental_update tests
# ---------------------------------------------------------------------------

# Columns that appear in most test batches after system field renaming.
_STANDARD_TABLE_COLUMNS = [
    "id",
    "val",
    "updated_on",
    "socrata_id",
    "socrata_updated_at",
    "socrata_created_at",
    "socrata_version",
    "ingested_at",
]


class TestIncrementalUpdate:
    CONFIG = IncrementalConfig(
        incremental_column="updated_on",
        entity_key=["id"],
    )

    def _setup_preflight(self, mock_engine, extra_columns: list[str] | None = None):
        """Stub table columns so the preflight check passes."""
        cols = list(_STANDARD_TABLE_COLUMNS)
        if extra_columns:
            cols.extend(extra_columns)
        stub_table_columns(mock_engine, cols)

    def test_basic_multi_page_ingest(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        self._setup_preflight(mock_engine)

        page1 = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        page2 = make_batch(
            {"id": "3", "updated_on": "2024-01-03", "val": "c"},
        )
        attach_mock_client(collector, [page1, page2])

        total = collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        assert total == 3

        call_kwargs = mock_engine.staged_ingest.call_args.kwargs
        assert call_kwargs["target_table"] == "test"
        assert call_kwargs["target_schema"] == "raw_data"
        assert call_kwargs["conflict_column"] == ["id"]

        stager = mock_engine._stagers[0]
        assert len(stager.batches) == 2
        assert len(stager.batches[0]) == 2
        assert len(stager.batches[1]) == 1

    def test_hwm_override_appears_in_where_clause(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        mock_client = attach_mock_client(collector, [])

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
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        config = IncrementalConfig(
            incremental_column="updated_on",
            entity_key=["id"],
            where="status = 'active'",
        )
        mock_client = attach_mock_client(collector, [])

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
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        self._setup_preflight(mock_engine)

        batch = make_batch(
            {"id": "1", "updated_on": "2024-01-01"},
            {"id": "2", "updated_on": "2024-03-15"},
            {"id": "3", "updated_on": "2024-02-10"},
        )
        attach_mock_client(collector, [batch])

        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        run = collector.tracker.last_run
        assert run.status == "success"
        assert run.rows_ingested == 3
        assert run.rows_staged == 3
        assert run.rows_merged == 3
        assert run.high_water_mark is not None
        assert "2024-03-15" in run.high_water_mark

    def test_failure_propagates_and_is_tracked(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        mock_client = MagicMock(spec=SocrataClient)
        mock_client.paginate.side_effect = RuntimeError("API down")
        collector._client = mock_client

        with pytest.raises(RuntimeError, match="API down"):
            collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        run = collector.tracker.last_run
        assert run.status == "failed"
        assert "API down" in run.error

    def test_empty_result_returns_zero(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        attach_mock_client(collector, [])

        total = collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)
        assert total == 0

    def test_system_fields_renamed_in_batches(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        stub_table_columns(
            mock_engine,
            [
                "val",
                "socrata_id",
                "socrata_updated_at",
                "socrata_created_at",
                "socrata_version",
                "ingested_at",
            ],
        )

        batch = make_batch(
            {":id": "row1", ":updated_at": "2024-01-01T00:00:00", "val": "x"},
        )
        attach_mock_client(collector, [batch])

        config = IncrementalConfig(
            incremental_column=":updated_at",
            entity_key=["socrata_id"],
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
        """HWM from target table should produce keyset pagination."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()

        collector._get_hwm_from_table = lambda *args: ("2024-06-01", "row99")

        mock_client = attach_mock_client(collector, [])
        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        where = mock_client.paginate.call_args[1]["where"]
        assert "2024-06-01" in where
        assert "row99" in where


# ---------------------------------------------------------------------------
# SocrataCollector.full_refresh_via_api tests
# ---------------------------------------------------------------------------


class TestFullRefreshViaApi:
    def test_delegates_to_incremental_update(self, collector, mock_engine):
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        attach_mock_client(collector, [])

        total = collector.full_refresh_via_api("abcd-1234", "test", "raw_data")

        assert total == 0
        collector._client.paginate.assert_called_once()


# ---------------------------------------------------------------------------
# SocrataCollector.preview tests
# ---------------------------------------------------------------------------


class TestPreview:
    def test_delegates_to_client_query(self, collector):
        collector._metadata_cache["wxyz-5678"] = make_metadata_mock()

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
# Metadata cache test
# ---------------------------------------------------------------------------


class TestMetadataCache:
    def test_caches_metadata_instance(self, collector):
        meta = make_metadata_mock()
        collector._metadata_cache["abcd-1234"] = meta

        assert collector._get_metadata("abcd-1234") is meta
        assert collector._get_metadata("abcd-1234") is meta  # same object
