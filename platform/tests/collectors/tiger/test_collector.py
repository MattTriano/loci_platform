"""Behavioral tests for TigerCollector orchestration logic."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
#  Minimal fakes for collaborators
# ---------------------------------------------------------------------------


def _make_spec(**overrides):
    """Build a TigerDatasetSpec-like object with sensible defaults."""
    defaults = dict(
        name="census_tracts",
        layer="TRACT",
        vintages=[2024],
        target_table="census_tracts",
        target_schema="raw_data",
        source="tiger",
        scope="state",
        resolution=None,
        state_fips=["17"],
        lowercase_columns=True,
        entity_key=None,
    )
    defaults.update(overrides)

    spec = MagicMock()
    for k, v in defaults.items():
        setattr(spec, k, v)

    # TigerDatasetSpec exposes states as the canonical property for
    # the list of state FIPS codes to iterate over.
    if "states" not in overrides:
        spec.states = defaults["state_fips"]

    return spec


def _make_engine(already_ingested=False):
    """
    Build a fake engine.

    - query() returns a non-empty df when already_ingested=True
    - staged_ingest() returns a context manager with a write_batch stub
    """
    engine = MagicMock()

    df = MagicMock()
    df.empty = not already_ingested
    engine.query.return_value = df

    # Track all stagers created so tests can inspect them
    engine._stagers = []

    def _make_stager():
        stager = MagicMock()
        stager.rows_staged = 0
        stager.rows_merged = 0
        stager._written_batches = []

        def _write_batch(batch):
            stager._written_batches.append(batch)
            stager.rows_staged += len(batch)
            stager.rows_merged += len(batch)

        stager.write_batch.side_effect = _write_batch
        engine._stagers.append(stager)
        return stager

    @contextmanager
    def _staged_ingest(**kwargs):
        stager = _make_stager()
        stager._ingest_kwargs = kwargs
        yield stager

    engine.staged_ingest.side_effect = _staged_ingest

    return engine


def _fake_parse_shapefile(filepath, **kwargs):
    """Return a one-batch iterator with two fake rows (has geoid and statefp)."""
    rows = [
        {"geoid": "17031000100", "statefp": "17", "geom": "DEADBEEF"},
        {"geoid": "17031000200", "statefp": "17", "geom": "CAFEBABE"},
    ]

    def _batches():
        yield rows

    result = MagicMock()
    result.features_parsed = 2
    result.features_failed = 0
    result.source_crs = "EPSG:4326"

    return _batches(), result


def _fake_parse_no_geoid(filepath, **kwargs):
    """Return rows without a GEOID column (e.g. coastline)."""
    rows = [
        {"name": "Atlantic", "mtfcc": "C10", "geom": "DEADBEEF"},
        {"name": "Pacific", "mtfcc": "C10", "geom": "CAFEBABE"},
    ]

    def _batches():
        yield rows

    result = MagicMock()
    result.features_parsed = 2
    result.features_failed = 0
    result.source_crs = "EPSG:4326"

    return _batches(), result


def _fake_parse_county_no_fips(filepath, **kwargs):
    """Return rows for a county-scoped layer missing statefp/countyfp (e.g. ADDR)."""
    rows = [
        {"tlid": "123", "fromhn": "100", "tohn": "200", "zip": "60601"},
        {"tlid": "456", "fromhn": "300", "tohn": "400", "zip": "60602"},
    ]

    def _batches():
        yield rows

    result = MagicMock()
    result.features_parsed = 2
    result.features_failed = 0
    result.source_crs = None

    return _batches(), result


def _fake_parse_county_with_fips(filepath, **kwargs):
    """Return rows for a county-scoped layer that already has statefp/countyfp."""
    rows = [
        {
            "linearid": "ABC",
            "statefp": "17",
            "countyfp": "031",
            "fullname": "Main St",
            "geom": "DEADBEEF",
        },
        {
            "linearid": "DEF",
            "statefp": "17",
            "countyfp": "031",
            "fullname": "Oak Ave",
            "geom": "CAFEBABE",
        },
    ]

    def _batches():
        yield rows

    result = MagicMock()
    result.features_parsed = 2
    result.features_failed = 0
    result.source_crs = "EPSG:4326"

    return _batches(), result


# ---------------------------------------------------------------------------
#  Fixtures
# ---------------------------------------------------------------------------

# We patch at the module level where things are looked up.
COLLECTOR_MODULE = "loci.collectors.tiger.collector"


@pytest.fixture
def collector_deps():
    """
    Patch external dependencies and return (collector, engine, metadata).

    Patches: parse_shapefile, requests.Session, TigerMetadata, tempfile download.
    """
    with (
        patch(f"{COLLECTOR_MODULE}.TigerMetadata") as MockMetadata,
        patch(f"{COLLECTOR_MODULE}.requests") as mock_requests,
    ):
        metadata = MockMetadata.return_value
        metadata.get_download_url.return_value = "https://example.com/fake.zip"

        engine = _make_engine()

        # Import here so the patches are active
        from loci.collectors.tiger.collector import TigerCollector

        collector = TigerCollector(engine=engine)

        # Stub out _download_to_tempfile to avoid real HTTP
        tmp_path = Path("/tmp/fake_tiger.zip")
        collector._download_to_tempfile = MagicMock(return_value=tmp_path)

        # parse_shapefile is imported lazily inside _download_parse_ingest
        with patch(
            "loci.parsers.shapefile.parse_shapefile",
            side_effect=_fake_parse_shapefile,
        ):
            with patch.object(Path, "unlink"):
                yield collector, engine, metadata


@pytest.fixture
def collector_no_geoid():
    """Collector fixture where parse returns rows without GEOID."""
    with (
        patch(f"{COLLECTOR_MODULE}.TigerMetadata") as MockMetadata,
        patch(f"{COLLECTOR_MODULE}.requests"),
    ):
        metadata = MockMetadata.return_value
        metadata.get_download_url.return_value = "https://example.com/fake.zip"

        engine = _make_engine()

        from loci.collectors.tiger.collector import TigerCollector

        collector = TigerCollector(engine=engine)
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.zip"))

        with patch(
            "loci.parsers.shapefile.parse_shapefile",
            side_effect=_fake_parse_no_geoid,
        ):
            with patch.object(Path, "unlink"):
                yield collector, engine, metadata


@pytest.fixture
def collector_county_no_fips():
    """Collector fixture for county-scoped layer with no statefp/countyfp in data."""
    with (
        patch(f"{COLLECTOR_MODULE}.TigerMetadata") as MockMetadata,
        patch(f"{COLLECTOR_MODULE}.requests"),
    ):
        metadata = MockMetadata.return_value

        engine = _make_engine()

        from loci.collectors.tiger.collector import TigerCollector

        collector = TigerCollector(engine=engine)
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.zip"))

        with patch(
            "loci.parsers.shapefile.parse_shapefile",
            side_effect=_fake_parse_county_no_fips,
        ):
            with patch.object(Path, "unlink"):
                yield collector, engine, metadata


@pytest.fixture
def collector_county_with_fips():
    """Collector fixture for county-scoped layer that already has statefp/countyfp."""
    with (
        patch(f"{COLLECTOR_MODULE}.TigerMetadata") as MockMetadata,
        patch(f"{COLLECTOR_MODULE}.requests"),
    ):
        metadata = MockMetadata.return_value

        engine = _make_engine()

        from loci.collectors.tiger.collector import TigerCollector

        collector = TigerCollector(engine=engine)
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.zip"))

        with patch(
            "loci.parsers.shapefile.parse_shapefile",
            side_effect=_fake_parse_county_with_fips,
        ):
            with patch.object(Path, "unlink"):
                yield collector, engine, metadata


# ---------------------------------------------------------------------------
#  Scope routing
# ---------------------------------------------------------------------------


class TestScopeRouting:
    def test_national_scope_calls_national_path(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec(scope="national")

        summary = collector.collect(spec)

        assert summary["files_processed"] == 1
        # National scope passes state_fips=None to the URL builder
        metadata.get_download_url.assert_called_once()
        call_kwargs = metadata.get_download_url.call_args
        assert call_kwargs.kwargs.get("state_fips") is None or (
            call_kwargs[1].get("state_fips") is None
        )

    def test_state_scope_iterates_over_states(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec(scope="state", state_fips=["17", "06", "36"])
        spec.states = ["17", "06", "36"]

        summary = collector.collect(spec)

        assert summary["files_processed"] == 3
        assert metadata.get_download_url.call_count == 3

    def test_county_scope_filters_to_requested_states(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec(scope="county", state_fips=["17"])
        spec.states = ["17"]

        # Simulate listing county files — one in IL (17), one in CA (06)
        import pandas as pd

        files_df = pd.DataFrame(
            [
                {
                    "filename": "tl_2024_17031_roads.zip",
                    "url": "https://example.com/17031.zip",
                    "state_fips": "17",
                },
                {
                    "filename": "tl_2024_06037_roads.zip",
                    "url": "https://example.com/06037.zip",
                    "state_fips": "06",
                },
            ]
        )
        metadata.list_files.return_value = files_df

        summary = collector.collect(spec)

        # Only the IL county should be processed
        assert summary["files_processed"] == 1
        assert summary["files_skipped"] == 0


# ---------------------------------------------------------------------------
#  Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_skips_already_ingested_files(self, collector_deps):
        collector, engine, metadata = collector_deps

        # Make the idempotency check say "already ingested"
        df = MagicMock()
        df.empty = False
        engine.query.return_value = df

        spec = _make_spec()
        summary = collector.collect(spec)

        assert summary["files_skipped"] == 1
        assert summary["files_processed"] == 0

    def test_force_bypasses_idempotency(self, collector_deps):
        collector, engine, metadata = collector_deps

        # Mark as already ingested
        df = MagicMock()
        df.empty = False
        engine.query.return_value = df

        spec = _make_spec()
        summary = collector.collect(spec, force=True)

        assert summary["files_processed"] == 1
        assert summary["files_skipped"] == 0


# ---------------------------------------------------------------------------
#  Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_download_failure_records_error_and_continues(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec(state_fips=["17", "06"])
        spec.states = ["17", "06"]

        # First state fails download, second succeeds
        call_count = 0

        def _download_side_effect(url):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("network down")
            return Path("/tmp/fake_tiger.zip")

        collector._download_to_tempfile = MagicMock(
            side_effect=_download_side_effect,
        )

        summary = collector.collect(spec)

        assert summary["files_processed"] == 1
        assert len(summary["errors"]) == 1
        assert "network down" in summary["errors"][0]["error"]
        assert summary["errors"][0]["state_fips"] == "17"


# ---------------------------------------------------------------------------
#  Summary accounting
# ---------------------------------------------------------------------------


class TestSummaryAccounting:
    def test_staged_and_merged_counts_accumulate(self, collector_deps):
        collector, engine, metadata = collector_deps

        # Fresh engine so stager counts start at zero
        fresh_engine = _make_engine()
        collector.engine = fresh_engine

        spec = _make_spec(state_fips=["17", "06"])
        spec.states = ["17", "06"]

        summary = collector.collect(spec)

        # _fake_parse_shapefile yields 2 rows per file, 2 files
        assert summary["total_rows_staged"] == 4
        assert summary["total_rows_merged"] == 4
        assert summary["vintages_processed"] == 1


# ---------------------------------------------------------------------------
#  Temp file cleanup
# ---------------------------------------------------------------------------


class TestTempFileCleanup:
    def test_temp_file_deleted_after_successful_ingest(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec()

        with patch.object(Path, "unlink") as mock_unlink:
            with patch(
                "loci.parsers.shapefile.parse_shapefile",
                side_effect=_fake_parse_shapefile,
            ):
                collector.collect(spec)

        mock_unlink.assert_called()

    def test_temp_file_deleted_even_on_ingest_failure(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec()

        with patch.object(Path, "unlink") as mock_unlink:
            with patch(
                "loci.parsers.shapefile.parse_shapefile",
                side_effect=Exception("parse boom"),
            ):
                summary = collector.collect(spec)

        mock_unlink.assert_called()
        assert len(summary["errors"]) == 1


# ---------------------------------------------------------------------------
#  Vintage column injection
# ---------------------------------------------------------------------------


class TestVintageInjection:
    def test_rows_receive_vintage_column(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec(vintages=[2023])

        collector.collect(spec)

        stagers = engine._stagers
        assert len(stagers) > 0
        for stager in stagers:
            for batch in stager._written_batches:
                for row in batch:
                    assert row["vintage"] == 2023


# ---------------------------------------------------------------------------
#  County FIPS extraction (pure logic, no mocks needed)
# ---------------------------------------------------------------------------


class TestExtractCountyFips:
    def _extract(self, filename, vintage):
        from loci.collectors.tiger.collector import TigerCollector

        return TigerCollector._extract_county_fips(filename, vintage)

    def test_extracts_five_digit_fips(self):
        assert self._extract("tl_2024_17031_roads.zip", 2024) == "17031"

    def test_returns_none_for_state_level_filename(self):
        # State-level files have 2-digit FIPS: tl_2024_17_tract.zip
        assert self._extract("tl_2024_17_tract.zip", 2024) is None

    def test_returns_none_for_national_filename(self):
        assert self._extract("tl_2024_us_state.zip", 2024) is None

    def test_wrong_vintage_returns_none(self):
        assert self._extract("tl_2023_17031_roads.zip", 2024) is None


# ---------------------------------------------------------------------------
#  Entity key resolution
# ---------------------------------------------------------------------------


class TestResolveEntityKey:
    def _resolve(self, spec, columns):
        from loci.collectors.tiger.collector import TigerCollector

        return TigerCollector._resolve_entity_key(spec, columns)

    def test_explicit_entity_key_takes_priority(self):
        spec = _make_spec(entity_key=["linearid", "vintage"])
        result = self._resolve(spec, ["geoid", "statefp", "geom"])
        assert result == ["linearid", "vintage"]

    def test_auto_detects_geoid(self):
        spec = _make_spec(entity_key=None)
        result = self._resolve(spec, ["geoid", "statefp", "geom"])
        assert result == ["geoid", "vintage"]

    def test_auto_detects_linearid(self):
        spec = _make_spec(entity_key=None)
        result = self._resolve(spec, ["linearid", "fullname", "geom"])
        assert result == ["linearid", "vintage"]

    def test_returns_none_when_no_candidates(self):
        spec = _make_spec(entity_key=None)
        result = self._resolve(spec, ["name", "mtfcc", "geom"])
        assert result is None

    def test_respects_lowercase_false(self):
        spec = _make_spec(entity_key=None, lowercase_columns=False)
        result = self._resolve(spec, ["GEOID", "STATEFP", "geom"])
        assert result == ["GEOID", "vintage"]


# ---------------------------------------------------------------------------
#  Non-SCD2 mode
# ---------------------------------------------------------------------------


class TestNonSCD2Mode:
    def test_no_entity_key_uses_simple_metadata_columns(self, collector_no_geoid):
        collector, engine, metadata = collector_no_geoid
        spec = _make_spec(scope="national")

        collector.collect(spec)

        stager = engine._stagers[0]
        ingest_kwargs = stager._ingest_kwargs
        assert ingest_kwargs["entity_key"] is None
        assert "record_hash" not in ingest_kwargs["metadata_columns"]
        assert "valid_from" not in ingest_kwargs["metadata_columns"]
        assert "valid_to" not in ingest_kwargs["metadata_columns"]
        assert "ingested_at" in ingest_kwargs["metadata_columns"]

    def test_with_entity_key_uses_full_metadata_columns(self, collector_deps):
        collector, engine, metadata = collector_deps
        spec = _make_spec()

        collector.collect(spec)

        stager = engine._stagers[0]
        ingest_kwargs = stager._ingest_kwargs
        assert ingest_kwargs["entity_key"] is not None
        assert "record_hash" in ingest_kwargs["metadata_columns"]
        assert "valid_from" in ingest_kwargs["metadata_columns"]
        assert "valid_to" in ingest_kwargs["metadata_columns"]


# ---------------------------------------------------------------------------
#  Synthetic FIPS injection
# ---------------------------------------------------------------------------


class TestSyntheticFipsInjection:
    def test_injects_statefp_and_countyfp_when_missing(self, collector_county_no_fips):
        """County-scoped layer with no statefp/countyfp gets them injected."""
        collector, engine, metadata = collector_county_no_fips
        spec = _make_spec(scope="county", state_fips=["17"])
        spec.states = ["17"]

        import pandas as pd

        files_df = pd.DataFrame(
            [
                {
                    "filename": "tl_2024_17031_addr.zip",
                    "url": "https://example.com/17031.zip",
                    "state_fips": "17",
                },
            ]
        )
        metadata.list_files.return_value = files_df

        collector.collect(spec)

        stager = engine._stagers[0]
        for batch in stager._written_batches:
            for row in batch:
                assert row["statefp"] == "17"
                assert row["countyfp"] == "031"

    def test_does_not_inject_fips_when_already_present(self, collector_county_with_fips):
        """County-scoped layer that already has statefp/countyfp keeps originals."""
        collector, engine, metadata = collector_county_with_fips
        spec = _make_spec(scope="county", state_fips=["17"])
        spec.states = ["17"]

        import pandas as pd

        files_df = pd.DataFrame(
            [
                {
                    "filename": "tl_2024_17031_roads.zip",
                    "url": "https://example.com/17031.zip",
                    "state_fips": "17",
                },
            ]
        )
        metadata.list_files.return_value = files_df

        collector.collect(spec)

        stager = engine._stagers[0]
        for batch in stager._written_batches:
            for row in batch:
                # Values should be from the original data, not overwritten
                assert row["statefp"] == "17"
                assert row["countyfp"] == "031"

    def test_no_fips_injection_for_state_scoped_layers(self, collector_deps):
        """State-scoped layers should not get synthetic FIPS."""
        collector, engine, metadata = collector_deps
        spec = _make_spec(scope="state")

        collector.collect(spec)

        stager = engine._stagers[0]
        for batch in stager._written_batches:
            for row in batch:
                # statefp is in the original data but countyfp should not be added
                assert "countyfp" not in row
