"""Tests for CensusDatasetSpec."""

import pytest
from loci.collectors.census.spec import ALL_STATE_FIPS, GEOGRAPHY_CONFIG, CensusDatasetSpec

# ------------------------------------------------------------------ #
#  Validation
# ------------------------------------------------------------------ #


class TestValidation:
    def test_requires_at_least_one_group_or_variable(self):
        with pytest.raises(ValueError, match="at least one group or variable"):
            CensusDatasetSpec(
                name="empty",
                dataset="acs/acs5",
                vintages=[2022],
                groups=[],
                variables=[],
                geography_level="tract",
                target_table="t",
                target_schema="raw_data",
            )

    def test_rejects_unknown_geography_level(self):
        with pytest.raises(ValueError, match="Unknown geography_level"):
            CensusDatasetSpec(
                name="bad_geo",
                dataset="acs/acs5",
                vintages=[2022],
                groups=["B24010"],
                geography_level="neighborhood",
                target_table="t",
                target_schema="raw_data",
            )

    def test_accepts_all_supported_geography_levels(self):
        for level in GEOGRAPHY_CONFIG:
            spec = CensusDatasetSpec(
                name="test",
                dataset="acs/acs5",
                vintages=[2022],
                groups=["B24010"],
                geography_level=level,
                target_table="t",
                target_schema="raw_data",
            )
            assert spec.geography_level == level

    def test_groups_only_is_valid(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        assert spec.groups == ["B24010"]
        assert spec.variables == []

    def test_variables_only_is_valid(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            variables=["B01001_001E"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        assert spec.variables == ["B01001_001E"]
        assert spec.groups == []


# ------------------------------------------------------------------ #
#  Derived state
# ------------------------------------------------------------------ #


class TestEntityKey:
    def test_tract_entity_key_includes_geo_columns_and_vintage(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        assert spec.entity_key == ["state", "county", "tract", "vintage"]

    def test_county_entity_key(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="county",
            target_table="t",
            target_schema="raw_data",
        )
        assert spec.entity_key == ["state", "county", "vintage"]

    def test_zcta_entity_key_has_no_state(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="zip code tabulation area",
            target_table="t",
            target_schema="raw_data",
        )
        assert "state" not in spec.entity_key
        assert spec.entity_key == ["zip code tabulation area", "vintage"]


# ------------------------------------------------------------------ #
#  States property
# ------------------------------------------------------------------ #


class TestStatesProperty:
    def test_defaults_to_all_states(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )
        assert spec.states == ALL_STATE_FIPS

    def test_uses_specified_state_fips(self):
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
            state_fips=["17", "18"],
        )
        assert spec.states == ["17", "18"]
