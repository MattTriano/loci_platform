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

import pytest
from loci.collectors.osm.metadata import OsmMetadata

# ================================================================== #
#  OsmMetadata — real Geofabrik API
# ================================================================== #


@pytest.fixture(scope="module")
def metadata():
    """Shared OsmMetadata instance (caches the index across tests)."""
    return OsmMetadata()


@pytest.mark.network
class TestOsmMetadataListRegions:
    """list_regions browsing behavior."""

    def test_top_level_returns_continents(self, metadata):
        df = metadata.list_regions()
        assert not df.empty
        # The index should contain well-known continents
        ids = set(df["id"])
        for continent in ("africa", "europe", "north-america", "asia"):
            assert continent in ids

    def test_top_level_has_no_parent(self, metadata):
        df = metadata.list_regions()
        assert (df["parent"] == "").all()

    def test_subregions_of_north_america(self, metadata):
        df = metadata.list_regions("north-america")
        assert not df.empty
        assert (df["parent"] == "north-america").all()
        ids = set(df["id"])
        assert "us" in ids or "canada" in ids

    def test_keyword_filters_within_parent(self, metadata):
        df = metadata.list_regions("europe", keyword="germany")
        assert not df.empty
        assert any("germany" in row.lower() for row in df["id"])

    def test_nonexistent_parent_returns_empty(self, metadata):
        df = metadata.list_regions("nonexistent-region-xyz")
        assert df.empty

    def test_result_columns(self, metadata):
        df = metadata.list_regions()
        assert set(df.columns) == {"id", "name", "parent", "iso", "pbf_url"}

    def test_results_are_sorted_by_name(self, metadata):
        df = metadata.list_regions("europe")
        names = list(df["name"])
        assert names == sorted(names)


@pytest.mark.network
class TestOsmMetadataSearch:
    """search behavior across all regions."""

    def test_search_by_name(self, metadata):
        df = metadata.search("illinois")
        assert not df.empty
        assert "us/illinois" in set(df["id"])

    def test_search_is_case_insensitive(self, metadata):
        df_lower = metadata.search("illinois")
        df_upper = metadata.search("ILLINOIS")
        assert set(df_lower["id"]) == set(df_upper["id"])

    def test_search_by_iso_code(self, metadata):
        df = metadata.search("DE", by="iso")
        assert not df.empty
        # Germany should be in the results
        assert any("DE" in row for row in df["iso"])

    def test_search_no_results(self, metadata):
        df = metadata.search("zzz-nonexistent-region-zzz")
        assert df.empty


@pytest.mark.network
class TestOsmMetadataDownloadUrl:
    """get_download_url behavior."""

    def test_returns_pbf_url_for_known_region(self, metadata):
        url = metadata.get_download_url("us/illinois")
        assert url.endswith(".osm.pbf")
        assert "illinois" in url

    def test_default_format_is_pbf(self, metadata):
        url = metadata.get_download_url("germany")
        assert ".osm.pbf" in url

    def test_unknown_region_raises_key_error(self, metadata):
        with pytest.raises(KeyError, match="not found"):
            metadata.get_download_url("nonexistent-region-xyz")

    def test_unknown_format_raises_key_error(self, metadata):
        with pytest.raises(KeyError, match="not available"):
            metadata.get_download_url("us/illinois", fmt="geotiff")


@pytest.mark.network
class TestOsmMetadataGetRegion:
    """get_region behavior."""

    def test_returns_dict_with_expected_keys(self, metadata):
        region = metadata.get_region("us/illinois")
        assert region["id"] == "us/illinois"
        assert "name" in region
        assert "urls" in region
        assert "pbf" in region["urls"]

    def test_unknown_region_raises_key_error(self, metadata):
        with pytest.raises(KeyError, match="not found"):
            metadata.get_region("nonexistent-region-xyz")
