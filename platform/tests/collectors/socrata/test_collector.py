from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from loci.collectors.config import IncrementalConfig
from loci.collectors.exceptions import SchemaDriftError
from loci.collectors.socrata.client import SocrataClient
from loci.collectors.socrata.collector import SocrataCollector

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


class TestSystemFieldsIngested:
    """
    Regression: Socrata system fields (socrata_id, socrata_updated_at, etc.)
    must be written to the staging table with their values intact, not dropped
    or nulled during ingestion.
    """

    def test_system_field_values_preserved_in_staged_batches(self, collector, mock_engine):
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
            {
                ":id": "row-abc",
                ":updated_at": "2024-06-15T12:00:00",
                ":created_at": "2024-01-01T00:00:00",
                ":version": "42",
                "id": "1",
                "val": "x",
            },
        )
        attach_mock_client(collector, [batch])

        config = IncrementalConfig(
            incremental_column=":updated_at",
            entity_key=["id"],
        )
        collector.incremental_update("abcd-1234", "test", "raw_data", config)

        stager = mock_engine._stagers[0]
        assert len(stager.batches) == 1
        row = stager.batches[0][0]

        assert row["socrata_id"] == "row-abc"
        assert row["socrata_updated_at"] == "2024-06-15T12:00:00"
        assert row["socrata_created_at"] == "2024-01-01T00:00:00"
        assert row["socrata_version"] == "42"


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


# ---------------------------------------------------------------------------
# max_rows cap tests
# ---------------------------------------------------------------------------


class TestMaxRowsCap:
    """Tests for the optional row cap on incremental runs."""

    CONFIG = IncrementalConfig(
        incremental_column="updated_on",
        entity_key=["id"],
    )

    def _setup_preflight(self, mock_engine):
        stub_table_columns(mock_engine, _STANDARD_TABLE_COLUMNS)

    def test_stops_at_page_boundary_when_cap_equals_page(self, collector, mock_engine):
        """Cap exactly at page size: one page stages, no further pages fetched."""
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

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG, max_rows=2
        )

        assert total == 2
        stager = mock_engine._stagers[0]
        assert stager.rows_staged == 2
        assert len(stager.batches) == 1

    def test_overshoots_to_finish_page_when_cap_hit_mid_page(self, collector, mock_engine):
        """Cap between page boundaries: finishes the current page, then stops."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        self._setup_preflight(mock_engine)

        page1 = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        page2 = make_batch(
            {"id": "3", "updated_on": "2024-01-03", "val": "c"},
            {"id": "4", "updated_on": "2024-01-04", "val": "d"},
        )
        page3 = make_batch(
            {"id": "5", "updated_on": "2024-01-05", "val": "e"},
        )
        attach_mock_client(collector, [page1, page2, page3])

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG, max_rows=3
        )

        # Cap is 3, but page 2 pushes us to 4; we stop there without fetching page 3.
        assert total == 4
        stager = mock_engine._stagers[0]
        assert stager.rows_staged == 4
        assert len(stager.batches) == 2

    def test_cap_larger_than_available_rows_ingests_everything(self, collector, mock_engine):
        """Cap above total row count behaves like no cap."""
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

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG, max_rows=1000
        )

        assert total == 3
        stager = mock_engine._stagers[0]
        assert stager.rows_staged == 3

    def test_cap_hit_recorded_in_run_metadata(self, collector, mock_engine):
        """When the cap fires, run.metadata['max_rows_hit'] is True."""
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

        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG, max_rows=2)

        run = collector.tracker.last_run
        assert run.metadata.get("max_rows_hit") is True

    def test_cap_not_hit_is_not_recorded_in_run_metadata(self, collector, mock_engine):
        """When the run finishes naturally, max_rows_hit is absent or False."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        self._setup_preflight(mock_engine)

        batch = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
        )
        attach_mock_client(collector, [batch])

        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG, max_rows=1000)

        run = collector.tracker.last_run
        assert not run.metadata.get("max_rows_hit", False)

    def test_hwm_after_capped_run_reflects_last_staged_page(self, collector, mock_engine):
        """HWM advances through fully-staged pages so a follow-up run resumes correctly."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        self._setup_preflight(mock_engine)

        page1 = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        # Page 2 would advance the HWM further, but we cap before fetching it.
        page2 = make_batch(
            {"id": "3", "updated_on": "2024-06-15", "val": "c"},
        )
        attach_mock_client(collector, [page1, page2])

        collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG, max_rows=2)

        run = collector.tracker.last_run
        assert run.high_water_mark is not None
        assert "2024-01-02" in run.high_water_mark
        assert "2024-06-15" not in run.high_water_mark

    def test_explicit_argument_overrides_instance_default(self, mock_engine):
        """max_rows passed to incremental_update wins over SocrataCollector(max_rows=...)."""
        collector = SocrataCollector(engine=mock_engine, page_size=100, max_rows=1000)
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        stub_table_columns(mock_engine, _STANDARD_TABLE_COLUMNS)

        page1 = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        page2 = make_batch(
            {"id": "3", "updated_on": "2024-01-03", "val": "c"},
        )
        attach_mock_client(collector, [page1, page2])

        total = collector.incremental_update(
            "abcd-1234", "test", "raw_data", self.CONFIG, max_rows=2
        )

        assert total == 2

    def test_instance_default_applies_when_no_argument_passed(self, mock_engine):
        """SocrataCollector(max_rows=...) caps a run with no explicit arg."""
        collector = SocrataCollector(engine=mock_engine, page_size=100, max_rows=2)
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()
        stub_table_columns(mock_engine, _STANDARD_TABLE_COLUMNS)

        page1 = make_batch(
            {"id": "1", "updated_on": "2024-01-01", "val": "a"},
            {"id": "2", "updated_on": "2024-01-02", "val": "b"},
        )
        page2 = make_batch(
            {"id": "3", "updated_on": "2024-01-03", "val": "c"},
        )
        attach_mock_client(collector, [page1, page2])

        total = collector.incremental_update("abcd-1234", "test", "raw_data", self.CONFIG)

        assert total == 2

    def test_rejects_max_rows_on_full_refresh_via_api(self, collector, mock_engine):
        """max_rows with an empty HWM override (full refresh via API) must raise."""
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()

        with pytest.raises(ValueError, match="full_refresh_via_api"):
            collector.incremental_update(
                "abcd-1234",
                "test",
                "raw_data",
                self.CONFIG,
                high_water_mark_override="",
                max_rows=100,
            )

    def test_full_refresh_via_api_rejects_instance_max_rows(self, mock_engine):
        """Instance-level max_rows must not silently apply to full_refresh_via_api."""
        collector = SocrataCollector(engine=mock_engine, page_size=100, max_rows=100)
        collector._metadata_cache["abcd-1234"] = make_metadata_mock()

        with pytest.raises(ValueError, match="full_refresh_via_api"):
            collector.full_refresh_via_api("abcd-1234", "test", "raw_data")
