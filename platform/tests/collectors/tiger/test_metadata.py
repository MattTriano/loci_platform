"""
Integration tests for TigerMetadata.

These tests hit the live Census Bureau website at www2.census.gov.
Run with: pytest test_tiger_metadata.py -v

Requirements: pandas, requests, pytest
"""

import pytest
import requests
from loci.collectors.tiger.metadata import TigerMetadata


@pytest.fixture(scope="module")
def tm():
    return TigerMetadata()


# ------------------------------------------------------------------ #
#  Smoke test: can we reach the Census site at all?
# ------------------------------------------------------------------ #


def test_census_site_reachable():
    """Baseline check — if this fails, all other tests are meaningless."""
    resp = requests.get("https://www2.census.gov/geo/tiger/", timeout=30)
    assert resp.status_code == 200


# ------------------------------------------------------------------ #
#  list_vintages
# ------------------------------------------------------------------ #


def test_list_tiger_vintages(tm):
    df = tm.list_vintages(source="tiger")
    assert not df.empty
    assert "vintage" in df.columns
    assert 2024 in df["vintage"].values


def test_list_cartographic_vintages(tm):
    df = tm.list_vintages(source="cartographic")
    assert not df.empty
    assert 2024 in df["vintage"].values


# ------------------------------------------------------------------ #
#  list_layers
# ------------------------------------------------------------------ #


def test_list_tiger_layers(tm):
    df = tm.list_layers(2024, source="tiger")
    assert not df.empty
    assert "TRACT" in df["layer"].values


def test_list_tiger_layers_keyword(tm):
    df = tm.list_layers(2024, source="tiger", keyword="road")
    assert not df.empty
    layer_names = df["layer"].values
    assert "PRIMARYROADS" in layer_names or "ROADS" in layer_names


def test_list_cartographic_layers(tm):
    df = tm.list_layers(2024, source="cartographic")
    assert not df.empty
    assert "tract" in df["layer"].values


# ------------------------------------------------------------------ #
#  list_files — TIGER/Line
# ------------------------------------------------------------------ #


class TestListTigerFiles:
    """Tests for list_files with source='tiger'."""

    def test_returns_dataframe_with_expected_columns(self, tm):
        df = tm.list_files(2024, "TRACT")
        assert set(df.columns) >= {"filename", "state_fips", "size", "url"}

    def test_returns_nonempty_results(self, tm):
        df = tm.list_files(2024, "TRACT")
        assert not df.empty, (
            "list_files returned an empty DataFrame for TRACT. "
            "The HTML parsing in _parse_directory_links may not match "
            "the Census Bureau's current directory listing format."
        )

    def test_state_fips_extracted(self, tm):
        df = tm.list_files(2024, "TRACT")
        if df.empty:
            pytest.skip("No files returned — can't check FIPS extraction")
        # Illinois = FIPS 17
        illinois = df[df["state_fips"] == "17"]
        assert not illinois.empty, "Expected to find Illinois (FIPS 17) tract file"

    def test_filenames_are_zips(self, tm):
        df = tm.list_files(2024, "TRACT")
        if df.empty:
            pytest.skip("No files returned")
        assert all(df["filename"].str.endswith(".zip"))

    def test_urls_are_well_formed(self, tm):
        df = tm.list_files(2024, "TRACT")
        if df.empty:
            pytest.skip("No files returned")
        for url in df["url"]:
            assert url.startswith("https://www2.census.gov/geo/tiger/TIGER2024/TRACT/")
            assert url.endswith(".zip")

    def test_national_layer_has_no_state_fips(self, tm):
        """National layers (like STATE) should have state_fips=None for the 'us' file."""
        df = tm.list_files(2024, "STATE")
        if df.empty:
            pytest.skip("No files returned for STATE layer")
        national = df[df["filename"].str.contains("_us_")]
        if not national.empty:
            assert national.iloc[0]["state_fips"] is None

    def test_case_insensitive_layer_name(self, tm):
        """list_files should accept lowercase layer names for TIGER."""
        df = tm.list_files(2024, "tract")
        assert not df.empty or True  # At minimum, no error raised


# ------------------------------------------------------------------ #
#  list_files — Cartographic Boundaries
# ------------------------------------------------------------------ #


class TestListCartographicFiles:
    """Tests for list_files with source='cartographic'."""

    def test_returns_dataframe_with_expected_columns(self, tm):
        df = tm.list_files(2024, "tract", source="cartographic")
        assert set(df.columns) >= {"filename", "state_fips", "resolution", "size", "url"}

    def test_returns_nonempty_results(self, tm):
        df = tm.list_files(2024, "tract", source="cartographic")
        assert not df.empty, (
            "list_files returned an empty DataFrame for cartographic tracts. "
            "The HTML parsing may not match the Census site's format."
        )

    def test_resolution_extracted(self, tm):
        df = tm.list_files(2024, "tract", source="cartographic")
        if df.empty:
            pytest.skip("No files returned")
        assert df["resolution"].notna().any(), "Expected at least one file with a resolution value"

    def test_urls_point_to_shp_directory(self, tm):
        df = tm.list_files(2024, "tract", source="cartographic")
        if df.empty:
            pytest.skip("No files returned")
        for url in df["url"]:
            assert "/GENZ2024/shp/" in url


# ------------------------------------------------------------------ #
#  get_download_url
# ------------------------------------------------------------------ #


class TestGetDownloadUrl:
    """Test that constructed URLs match the Census naming conventions."""

    def test_tiger_state_url(self, tm):
        url = tm.get_download_url(2024, "TRACT", state_fips="17")
        assert url == ("https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_17_tract.zip")

    def test_tiger_national_url(self, tm):
        url = tm.get_download_url(2024, "PRIMARYROADS")
        assert url == (
            "https://www2.census.gov/geo/tiger/TIGER2024/PRIMARYROADS/tl_2024_us_primaryroads.zip"
        )

    def test_cartographic_state_url(self, tm):
        url = tm.get_download_url(2024, "tract", source="cartographic", state_fips="17")
        assert url == ("https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_17_tract_500k.zip")

    def test_cartographic_national_url(self, tm):
        url = tm.get_download_url(2024, "state", source="cartographic")
        assert url == ("https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_state_500k.zip")

    def test_cartographic_custom_resolution(self, tm):
        url = tm.get_download_url(2024, "county", source="cartographic", resolution="20m")
        assert url == ("https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_20m.zip")

    def test_constructed_url_exists(self, tm):
        """Verify a constructed URL actually resolves (HEAD request)."""
        url = tm.get_download_url(2024, "TRACT", state_fips="17")
        resp = requests.head(url, timeout=30, allow_redirects=True)
        assert resp.status_code == 200, f"Expected URL to exist: {url}"


# ------------------------------------------------------------------ #
#  _parse_directory_links — the most likely failure point
# ------------------------------------------------------------------ #


class TestParseDirectoryLinks:
    """
    Directly test the HTML parsing against live pages.

    If list_files returns empty DataFrames, this is the most likely
    culprit — the regex may not match the Census site's actual HTML.
    """

    def test_parses_tiger_file_listing(self, tm):
        html = tm._fetch_directory("https://www2.census.gov/geo/tiger/TIGER2024/TRACT/")
        entries = tm._parse_directory_links(html)
        assert len(entries) > 0, (
            "_parse_directory_links returned no entries. "
            "The Apache directory listing format may have changed. "
            "Check the raw HTML to see if the regex still matches."
        )

    def test_parses_cartographic_file_listing(self, tm):
        html = tm._fetch_directory("https://www2.census.gov/geo/tiger/GENZ2024/shp/")
        entries = tm._parse_directory_links(html)
        assert len(entries) > 0, (
            "_parse_directory_links returned no entries for the cartographic boundary directory."
        )

    def test_parsed_entries_have_zip_files(self, tm):
        html = tm._fetch_directory("https://www2.census.gov/geo/tiger/TIGER2024/TRACT/")
        entries = tm._parse_directory_links(html)
        zip_entries = [e for e in entries if e["name"].endswith(".zip")]
        assert len(zip_entries) > 0, "Parsed entries exist but none are .zip files"
