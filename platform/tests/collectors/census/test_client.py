"""Tests for CensusClient."""

from unittest.mock import patch

import pytest
from loci.collectors.census.client import CensusClient
from loci.collectors.census.spec import MAX_VARIABLES_PER_CALL, CensusDatasetSpec


@pytest.fixture
def client():
    return CensusClient(api_key="test_key")


# ------------------------------------------------------------------ #
#  Variable classification
# ------------------------------------------------------------------ #


class TestIsEstimateOrMoe:
    """Pure logic — test directly since it gates what gets collected."""

    def test_estimate(self, client):
        assert client._is_estimate_or_moe("B24010_001E") is True

    def test_moe(self, client):
        assert client._is_estimate_or_moe("B24010_001M") is True

    def test_rejects_estimate_annotation(self, client):
        assert client._is_estimate_or_moe("B24010_001EA") is False

    def test_rejects_moe_annotation(self, client):
        assert client._is_estimate_or_moe("B24010_001MA") is False

    def test_rejects_percent_estimate(self, client):
        assert client._is_estimate_or_moe("B24010_001PE") is False

    def test_rejects_percent_moe(self, client):
        assert client._is_estimate_or_moe("B24010_001PM") is False


# ------------------------------------------------------------------ #
#  Chunking
# ------------------------------------------------------------------ #


class TestChunkVariables:
    def test_small_list_returns_single_chunk(self):
        variables = [f"VAR_{i}" for i in range(10)]
        chunks = CensusClient._chunk_variables(variables)
        assert len(chunks) == 1
        assert chunks[0] == variables

    def test_splits_at_max_boundary(self):
        variables = [f"VAR_{i}" for i in range(MAX_VARIABLES_PER_CALL + 1)]
        chunks = CensusClient._chunk_variables(variables)
        assert len(chunks) == 2
        assert len(chunks[0]) == MAX_VARIABLES_PER_CALL
        assert len(chunks[1]) == 1

    def test_preserves_all_variables(self):
        variables = [f"VAR_{i}" for i in range(100)]
        chunks = CensusClient._chunk_variables(variables)
        flattened = [v for chunk in chunks for v in chunk]
        assert flattened == variables

    def test_empty_list(self):
        assert CensusClient._chunk_variables([]) == []


# ------------------------------------------------------------------ #
#  Group resolution
# ------------------------------------------------------------------ #


class TestResolveGroupVariables:
    def test_returns_sorted_variable_names(self, client):
        api_response = {
            "variables": {
                "B24010_001E": {"predicateType": "int"},
                "B24010_002E": {"predicateType": "int"},
                "B24010_001M": {"predicateType": "int"},
                "NAME": {"predicateType": "string"},
                "B24010": {"predicateType": None},  # group label entry
            }
        }
        with patch.object(client, "_get_json", return_value=api_response):
            result = client.resolve_group_variables("acs/acs5", 2022, "B24010")

        assert result == sorted(result)
        assert "NAME" not in result
        assert "B24010" not in result

    def test_excludes_entries_with_null_predicate_type(self, client):
        api_response = {
            "variables": {
                "B24010_001E": {"predicateType": "int"},
                "B24010_001EA": {"predicateType": None},
            }
        }
        with patch.object(client, "_get_json", return_value=api_response):
            result = client.resolve_group_variables("acs/acs5", 2022, "B24010")

        assert "B24010_001EA" not in result


# ------------------------------------------------------------------ #
#  resolve_all_variables
# ------------------------------------------------------------------ #


class TestResolveAllVariables:
    def test_combines_groups_and_individual_variables(self, client):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            variables=["B01001_001E"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        with patch.object(
            client,
            "resolve_group_variables",
            return_value=["B24010_001E", "B24010_001M", "B24010_001EA"],
        ):
            result = client.resolve_all_variables(spec, 2022)

        # EA filtered out, individual variable included
        assert "B24010_001E" in result
        assert "B24010_001M" in result
        assert "B01001_001E" in result
        assert "B24010_001EA" not in result

    def test_deduplicates_across_groups_and_variables(self, client):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            variables=["B24010_001E"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        with patch.object(
            client,
            "resolve_group_variables",
            return_value=["B24010_001E", "B24010_001M"],
        ):
            result = client.resolve_all_variables(spec, 2022)

        assert result.count("B24010_001E") == 1


# ------------------------------------------------------------------ #
#  fetch_variables — multi-chunk join
# ------------------------------------------------------------------ #


class TestFetchVariables:
    def test_single_chunk_returns_rows(self, client):
        api_rows = [
            ["NAME", "VAR_1", "VAR_2", "state", "county", "tract"],
            ["Tract 1", "100", "200", "17", "031", "000100"],
            ["Tract 2", "300", "400", "17", "031", "000200"],
        ]
        with patch.object(client, "_get_json", return_value=api_rows):
            result = client.fetch_variables(
                dataset="acs/acs5",
                vintage=2022,
                variables=["VAR_1", "VAR_2"],
                geography_level="tract",
                state_fips="17",
            )

        assert len(result) == 2
        assert result[0]["VAR_1"] == "100"
        assert result[1]["VAR_2"] == "400"

    def test_multi_chunk_joins_on_geo_columns(self, client):
        """When variables exceed MAX_VARIABLES_PER_CALL, chunks are joined by geo key."""
        # Build two chunks worth of variables with known names
        chunk1_vars = [f"VAR_{i}" for i in range(MAX_VARIABLES_PER_CALL)]
        chunk2_vars = [f"VAR_{MAX_VARIABLES_PER_CALL}"]
        all_vars = chunk1_vars + chunk2_vars

        geo_cols = ["state", "county", "tract"]
        geo_vals = ["17", "031", "000100"]

        chunk1_response = [
            ["NAME"] + chunk1_vars + geo_cols,
            ["Tract 1"] + ["100"] * len(chunk1_vars) + geo_vals,
        ]
        chunk2_response = [
            ["NAME"] + chunk2_vars + geo_cols,
            ["Tract 1", "200"] + geo_vals,
        ]

        responses = iter([chunk1_response, chunk2_response])

        with patch.object(client, "_get_json", side_effect=lambda *a, **kw: next(responses)):
            result = client.fetch_variables(
                dataset="acs/acs5",
                vintage=2022,
                variables=all_vars,
                geography_level="tract",
                state_fips="17",
            )

        assert len(result) == 1
        assert result[0]["VAR_0"] == "100"
        assert result[0][f"VAR_{MAX_VARIABLES_PER_CALL}"] == "200"

    def test_empty_response_returns_empty_list(self, client):
        with patch.object(client, "_get_json", return_value=[]):
            result = client.fetch_variables(
                dataset="acs/acs5",
                vintage=2022,
                variables=["VAR_1"],
                geography_level="tract",
                state_fips="17",
            )
        assert result == []

    def test_state_level_uses_state_fips_in_for_clause(self, client):
        """State-level queries put the FIPS in the 'for' clause, not 'in'."""
        api_rows = [
            ["NAME", "VAR_1", "state"],
            ["Illinois", "100", "17"],
        ]

        captured_params = {}

        def mock_get_json(url, params=None):
            captured_params.update(params or {})
            return api_rows

        with patch.object(client, "_get_json", side_effect=mock_get_json):
            client.fetch_variables(
                dataset="acs/acs5",
                vintage=2022,
                variables=["VAR_1"],
                geography_level="state",
                state_fips="17",
            )

        assert captured_params["for"] == "state:17"
        assert "in" not in captured_params
