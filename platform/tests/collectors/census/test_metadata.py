"""Tests for CensusMetadata."""

from unittest.mock import patch

import pytest
from loci.collectors.census.metadata import CensusMetadata


@pytest.fixture
def metadata():
    return CensusMetadata(api_key="test_key")


SAMPLE_CATALOG = [
    {
        "c_dataset": ["acs", "acs5"],
        "c_vintage": 2022,
        "title": "American Community Survey 5-Year Data",
        "description": "Detailed tables from the ACS 5-year estimates.",
        "c_isAvailable": True,
    },
    {
        "c_dataset": ["acs", "acs5"],
        "c_vintage": 2021,
        "title": "American Community Survey 5-Year Data",
        "description": "Detailed tables from the ACS 5-year estimates.",
        "c_isAvailable": True,
    },
    {
        "c_dataset": ["dec", "pl"],
        "c_vintage": 2020,
        "title": "Decennial Census Redistricting Data",
        "description": "PL 94-171 redistricting data.",
        "c_isAvailable": True,
    },
    {
        "c_dataset": ["acs", "acs1"],
        "c_vintage": 2022,
        "title": "American Community Survey 1-Year Data",
        "description": "ACS 1-year estimates.",
        "c_isAvailable": False,  # unavailable
    },
]


# ------------------------------------------------------------------ #
#  list_datasets
# ------------------------------------------------------------------ #


class TestListDatasets:
    def test_returns_only_available_datasets(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            df = metadata.list_datasets()

        # The unavailable acs1 entry should be excluded
        assert len(df) == 3
        assert "acs/acs1" not in df["name"].values

    def test_keyword_filters_by_title_and_description(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            df = metadata.list_datasets(keyword="redistricting")

        assert len(df) == 1
        assert df.iloc[0]["name"] == "dec/pl"

    def test_keyword_is_case_insensitive(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            df = metadata.list_datasets(keyword="COMMUNITY SURVEY")

        # Should match both acs/acs5 vintages
        assert all(name == "acs/acs5" for name in df["name"])

    def test_returns_expected_columns(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            df = metadata.list_datasets()

        for col in ["title", "name", "vintage", "description"]:
            assert col in df.columns


# ------------------------------------------------------------------ #
#  list_vintages
# ------------------------------------------------------------------ #


class TestListVintages:
    def test_returns_sorted_vintages_for_dataset(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            result = metadata.list_vintages("acs/acs5")

        assert result == [2021, 2022]

    def test_returns_empty_for_unknown_dataset(self, metadata):
        with patch.object(metadata, "_dataset_catalog", return_value=SAMPLE_CATALOG):
            result = metadata.list_vintages("nonexistent/dataset")

        assert result == []


# ------------------------------------------------------------------ #
#  list_groups
# ------------------------------------------------------------------ #


class TestListGroups:
    SAMPLE_GROUPS_RESPONSE = {
        "groups": [
            {"name": "B24010", "description": "Sex by Occupation for the Civilian Employed"},
            {"name": "B08105B", "description": "Means of Transportation to Work"},
            {"name": "B01001", "description": "Sex by Age"},
        ]
    }

    def test_returns_all_groups(self, metadata):
        with patch.object(metadata, "_get_json", return_value=self.SAMPLE_GROUPS_RESPONSE):
            df = metadata.list_groups("acs/acs5", 2022)

        assert len(df) == 3

    def test_keyword_filters_groups(self, metadata):
        with patch.object(metadata, "_get_json", return_value=self.SAMPLE_GROUPS_RESPONSE):
            df = metadata.list_groups("acs/acs5", 2022, keyword="occupation")

        assert len(df) == 1
        assert df.iloc[0]["group"] == "B24010"

    def test_results_sorted_by_group(self, metadata):
        with patch.object(metadata, "_get_json", return_value=self.SAMPLE_GROUPS_RESPONSE):
            df = metadata.list_groups("acs/acs5", 2022)

        assert list(df["group"]) == sorted(df["group"])


# ------------------------------------------------------------------ #
#  list_variables
# ------------------------------------------------------------------ #


class TestListVariables:
    SAMPLE_VARIABLES_RESPONSE = {
        "variables": {
            "B24010_001E": {
                "label": "Estimate!!Total:",
                "group": "B24010",
                "concept": "Sex by Occupation",
                "predicateType": "int",
            },
            "B24010_001M": {
                "label": "Margin of Error!!Total:",
                "group": "B24010",
                "concept": "Sex by Occupation",
                "predicateType": "int",
            },
            "NAME": {
                "label": "Geographic Area Name",
                "group": "",
                "concept": "",
                "predicateType": "string",
            },
        }
    }

    def test_returns_expected_columns(self, metadata):
        with patch.object(metadata, "_get_json", return_value=self.SAMPLE_VARIABLES_RESPONSE):
            df = metadata.list_variables("acs/acs5", 2022, group="B24010")

        for col in ["variable", "label", "group", "concept", "predicate_type"]:
            assert col in df.columns

    def test_keyword_filters_variables(self, metadata):
        with patch.object(metadata, "_get_json", return_value=self.SAMPLE_VARIABLES_RESPONSE):
            df = metadata.list_variables("acs/acs5", 2022, keyword="margin")

        assert len(df) == 1
        assert df.iloc[0]["variable"] == "B24010_001M"

    def test_group_url_differs_from_all_variables_url(self, metadata):
        """Verifies the correct API endpoint is called depending on whether group is set."""
        urls_called = []

        def capture_url(url):
            urls_called.append(url)
            return {"variables": {}}

        with patch.object(metadata, "_get_json", side_effect=capture_url):
            metadata.list_variables("acs/acs5", 2022, group="B24010")
            metadata.list_variables("acs/acs5", 2022)

        assert "groups/B24010" in urls_called[0]
        assert "groups" not in urls_called[1]
