"""
Behavioral tests for the OSM metadata, spec, and collector modules.

OsmMetadata tests hit the real Geofabrik API (marked with
pytest.mark.network so they can be skipped in CI with
`pytest -m "not network"`).

OsmDatasetSpec tests are pure unit tests.

OsmCollector and generate_ddl tests use mocks for the engine,
tracker, parse_pbf, and HTTP downloads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from loci.collectors.osm.collector import METADATA_COLUMNS, OsmCollector
from loci.collectors.osm.spec import OsmDatasetSpec

# ================================================================== #
#  OsmCollector — mocked tests
# ================================================================== #


@dataclass
class FakeParseResult:
    """Mimics ParseResult from pbf_parser."""

    features_parsed: int = 0
    features_failed: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)
    element_type: str = ""


class FakeStager:
    """Mimics the StagedIngest context manager."""

    def __init__(self):
        self.rows_staged = 0
        self.rows_merged = 0
        self.batches_written = []

    def write_batch(self, batch):
        self.batches_written.append(batch)
        self.rows_staged += len(batch)
        self.rows_merged += len(batch)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


@pytest.fixture
def mock_engine():
    """A mock PostgresEngine with a working staged_ingest."""
    engine = MagicMock()
    stager = FakeStager()
    engine.staged_ingest.return_value = stager
    engine._stager = stager  # expose for assertions
    return engine


@pytest.fixture
def node_spec():
    return OsmDatasetSpec(
        name="test_nodes",
        region_ids=["illinois"],
        element_type="nodes",
        target_table="osm_nodes",
    )


@pytest.fixture
def multi_region_spec():
    return OsmDatasetSpec(
        name="test_nodes",
        region_ids=["illinois", "indiana"],
        element_type="nodes",
        target_table="osm_nodes",
    )


def _make_fake_batches(n_batches=2, batch_size=3):
    """Return (batches_iterator, parse_result) mimicking parse_pbf output."""
    result = FakeParseResult(
        features_parsed=n_batches * batch_size,
        element_type="nodes",
    )

    def batches():
        for i in range(n_batches):
            yield [
                {"osm_id": i * batch_size + j, "tags": "{}", "geom": "0101..."}
                for j in range(batch_size)
            ]

    return batches(), result


class TestOsmCollectorCollect:
    """collect() orchestration behavior."""

    @patch("loci.collectors.osm.metadata")
    @patch("loci.parsers.pbf.parse_pbf")
    def test_collect_downloads_parses_and_ingests(
        self, mock_parse_pbf, MockMetadata, mock_engine, node_spec
    ):
        MockMetadata.return_value.get_download_url.return_value = (
            "https://example.com/illinois.osm.pbf"
        )
        batches, result = _make_fake_batches()
        mock_parse_pbf.return_value = (batches, result)

        collector = OsmCollector(engine=mock_engine)
        collector._metadata = MockMetadata.return_value
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.osm.pbf"))

        # Pretend the table doesn't exist yet (not already ingested)
        mock_engine.query.side_effect = Exception("table does not exist")

        summary = collector.collect(node_spec)

        assert summary["regions_processed"] == 1
        assert summary["total_features_parsed"] == 6
        assert summary["errors"] == []

    @patch("loci.collectors.osm.metadata")
    @patch("loci.parsers.pbf.parse_pbf")
    def test_collect_skips_already_ingested_region(
        self, mock_parse_pbf, MockMetadata, mock_engine, node_spec
    ):
        # Engine returns a non-empty result → already ingested
        mock_engine.query.return_value = pd.DataFrame({"x": [1]})

        collector = OsmCollector(engine=mock_engine)
        collector._metadata = MockMetadata.return_value
        summary = collector.collect(node_spec)

        assert summary["regions_skipped"] == 1
        assert summary["regions_processed"] == 0
        mock_parse_pbf.assert_not_called()

    @patch("loci.collectors.osm.metadata")
    @patch("loci.parsers.pbf.parse_pbf")
    def test_force_skips_idempotency_check(
        self, mock_parse_pbf, MockMetadata, mock_engine, node_spec
    ):
        # Even though data exists, force=True should proceed
        mock_engine.query.return_value = pd.DataFrame({"x": [1]})
        MockMetadata.return_value.get_download_url.return_value = (
            "https://example.com/illinois.osm.pbf"
        )
        batches, result = _make_fake_batches()
        mock_parse_pbf.return_value = (batches, result)

        collector = OsmCollector(engine=mock_engine)
        collector._metadata = MockMetadata.return_value
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.osm.pbf"))

        summary = collector.collect(node_spec, force=True)

        assert summary["regions_processed"] == 1
        assert summary["regions_skipped"] == 0

    @patch("loci.collectors.osm.metadata")
    @patch("loci.parsers.pbf.parse_pbf")
    def test_collect_iterates_multiple_regions(
        self, mock_parse_pbf, MockMetadata, mock_engine, multi_region_spec
    ):
        MockMetadata.return_value.get_download_url.return_value = (
            "https://example.com/region.osm.pbf"
        )
        mock_parse_pbf.side_effect = [
            _make_fake_batches(),
            _make_fake_batches(),
        ]
        mock_engine.query.side_effect = Exception("table does not exist")

        collector = OsmCollector(engine=mock_engine)
        collector._metadata = MockMetadata.return_value
        collector._download_to_tempfile = MagicMock(return_value=Path("/tmp/fake.osm.pbf"))

        summary = collector.collect(multi_region_spec)

        assert summary["regions_processed"] == 2
        assert summary["total_features_parsed"] == 12

    @patch("loci.collectors.osm.metadata")
    def test_download_failure_records_error_and_continues(
        self, MockMetadata, mock_engine, multi_region_spec
    ):
        MockMetadata.return_value.get_download_url.return_value = (
            "https://example.com/region.osm.pbf"
        )
        mock_engine.query.side_effect = Exception("table does not exist")

        collector = OsmCollector(engine=mock_engine)
        collector._metadata = MockMetadata.return_value
        collector._download_to_tempfile = MagicMock(side_effect=Exception("connection refused"))

        summary = collector.collect(multi_region_spec)

        assert len(summary["errors"]) == 2
        assert all(e["stage"] == "download" for e in summary["errors"])
        assert summary["regions_processed"] == 0


class TestOsmCollectorIngestBatches:
    """_ingest_batches adds region_id and feeds the stager."""

    @patch("loci.collectors.osm.metadata")
    def test_region_id_is_injected_into_rows(self, MockMetadata, mock_engine, node_spec):
        collector = OsmCollector(engine=mock_engine)
        batches = [
            [{"osm_id": 1, "tags": "{}"}, {"osm_id": 2, "tags": "{}"}],
        ]

        collector._ingest_batches(node_spec, iter(batches), "illinois")

        stager = mock_engine._stager
        written_rows = [row for batch in stager.batches_written for row in batch]
        assert all(row["region_id"] == "illinois" for row in written_rows)

    @patch("loci.collectors.osm.metadata")
    def test_staged_ingest_called_with_correct_params(self, MockMetadata, mock_engine, node_spec):
        collector = OsmCollector(engine=mock_engine)
        batches = [[{"osm_id": 1, "tags": "{}"}]]

        collector._ingest_batches(node_spec, iter(batches), "illinois")

        mock_engine.staged_ingest.assert_called_once_with(
            target_table="osm_nodes",
            target_schema="raw_data",
            entity_key=["osm_id", "region_id"],
            metadata_columns=METADATA_COLUMNS,
        )


class TestOsmCollectorIdempotency:
    """_already_ingested behavior."""

    @patch("loci.collectors.osm.metadata")
    def test_returns_true_when_current_rows_exist(self, MockMetadata, mock_engine, node_spec):
        mock_engine.query.return_value = pd.DataFrame({"x": [1]})
        collector = OsmCollector(engine=mock_engine)
        assert collector._already_ingested(node_spec, "illinois") is True

    @patch("loci.collectors.osm.metadata")
    def test_returns_false_when_no_rows(self, MockMetadata, mock_engine, node_spec):
        mock_engine.query.return_value = pd.DataFrame()
        collector = OsmCollector(engine=mock_engine)
        assert collector._already_ingested(node_spec, "illinois") is False

    @patch("loci.collectors.osm.metadata")
    def test_returns_false_when_table_missing(self, MockMetadata, mock_engine, node_spec):
        mock_engine.query.side_effect = Exception("relation does not exist")
        collector = OsmCollector(engine=mock_engine)
        assert collector._already_ingested(node_spec, "illinois") is False
