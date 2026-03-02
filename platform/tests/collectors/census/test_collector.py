"""Tests for CensusCollector."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from loci.collectors.census.collector import CensusCollector, generate_ddl
from loci.collectors.census.spec import CensusDatasetSpec

# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #


@pytest.fixture
def spec():
    return CensusDatasetSpec(
        name="test_spec",
        dataset="acs/acs5",
        vintages=[2021, 2022],
        groups=["B24010"],
        geography_level="tract",
        target_table="test_table",
        target_schema="raw_data",
        state_fips=["17", "18"],
    )


@pytest.fixture
def single_vintage_spec():
    return CensusDatasetSpec(
        name="simple",
        dataset="acs/acs5",
        vintages=[2022],
        groups=["B24010"],
        geography_level="tract",
        target_table="simple_table",
        target_schema="raw_data",
        state_fips=["17"],
    )


def make_collector(
    fetch_return=None,
    resolve_return=None,
    already_ingested=False,
):
    """Build a CensusCollector with mocked client and engine."""
    client = MagicMock()
    client.resolve_all_variables.return_value = resolve_return or ["B24010_001E", "B24010_001M"]
    client.fetch_variables.return_value = fetch_return or [
        {
            "NAME": "Tract 1",
            "state": "17",
            "county": "031",
            "tract": "000100",
            "B24010_001E": "100",
        },
    ]
    client._sanitize_column_name.side_effect = lambda c: c.replace(" ", "_")

    engine = MagicMock()
    stager = MagicMock()
    stager.rows_staged = len(fetch_return or [{"dummy": 1}])
    stager.rows_merged = len(fetch_return or [{"dummy": 1}])
    engine.staged_ingest.return_value.__enter__ = MagicMock(return_value=stager)
    engine.staged_ingest.return_value.__exit__ = MagicMock(return_value=False)

    collector = CensusCollector(client=client, engine=engine)
    return collector


# ------------------------------------------------------------------ #
#  Orchestration: collect()
# ------------------------------------------------------------------ #


class TestCollect:
    def test_processes_all_vintages_and_states(self, spec):
        collector = make_collector()
        summary = collector.collect(spec)

        assert summary["vintages_processed"] == 2
        # 2 vintages * 2 states = 4
        assert summary["states_processed"] == 4

    def test_skips_already_ingested_states(self, single_vintage_spec):
        collector = make_collector()
        with patch.object(collector, "_already_ingested", return_value=True):
            summary = collector.collect(single_vintage_spec)

        assert summary["states_skipped"] == 1
        assert summary["states_processed"] == 0

    def test_force_bypasses_idempotency_check(self, single_vintage_spec):
        collector = make_collector()
        with patch.object(collector, "_already_ingested", return_value=True):
            summary = collector.collect(single_vintage_spec, force=True)

        assert summary["states_skipped"] == 0
        assert summary["states_processed"] == 1

    def test_accumulates_errors_without_stopping(self, spec):
        collector = make_collector()
        with patch.object(
            collector,
            "_collect_state_vintage",
            side_effect=RuntimeError("API down"),
        ):
            summary = collector.collect(spec)

        # Should have tried all 4 combinations and recorded errors for each
        assert len(summary["errors"]) == 4
        assert summary["states_processed"] == 0

    def test_adds_vintage_to_fetched_rows(self, single_vintage_spec):
        rows = [{"NAME": "Tract 1", "state": "17", "county": "031", "tract": "000100"}]
        collector = make_collector(fetch_return=rows)
        collector.collect(single_vintage_spec)

        # The collector should have mutated the rows to include vintage
        assert rows[0]["vintage"] == 2022

    def test_resolves_variables_once_per_vintage(self, spec):
        collector = make_collector()
        collector.collect(spec)

        # 2 vintages, each resolved once (not once per state)
        assert collector.client.resolve_all_variables.call_count == 2

    def test_summary_has_expected_keys(self, single_vintage_spec):
        collector = make_collector()
        summary = collector.collect(single_vintage_spec)

        expected_keys = {
            "spec_name",
            "vintages_processed",
            "states_processed",
            "states_skipped",
            "total_rows_staged",
            "total_rows_merged",
            "errors",
        }
        assert set(summary.keys()) == expected_keys


# ------------------------------------------------------------------ #
#  Idempotency check
# ------------------------------------------------------------------ #


class TestAlreadyIngested:
    def test_returns_false_when_table_does_not_exist(self, single_vintage_spec):
        collector = make_collector()
        collector.engine.query.side_effect = Exception("relation does not exist")

        result = collector._already_ingested(single_vintage_spec, 2022, "17")
        assert result is False

    def test_returns_false_when_no_current_rows(self, single_vintage_spec):
        collector = make_collector()
        collector.engine.query.return_value = pd.DataFrame()

        result = collector._already_ingested(single_vintage_spec, 2022, "17")
        assert result is False

    def test_returns_true_when_current_rows_exist(self, single_vintage_spec):
        collector = make_collector()
        collector.engine.query.return_value = pd.DataFrame({"?column?": [1]})

        result = collector._already_ingested(single_vintage_spec, 2022, "17")
        assert result is True


# ------------------------------------------------------------------ #
#  DDL generation
# ------------------------------------------------------------------ #


class TestGenerateDdl:
    def test_contains_schema_qualified_table_name(self):
        client = MagicMock()
        client.resolve_all_variables.return_value = ["B24010_001E"]
        client._is_estimate_or_moe.return_value = True
        client._column_type.return_value = "numeric"
        client._sanitize_column_name.side_effect = lambda c: c.replace(" ", "_")

        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="test_table",
            target_schema="raw_data",
        )

        ddl = generate_ddl(spec, client)
        assert "raw_data.test_table" in ddl

    def test_includes_geo_columns_for_geography_level(self):
        client = MagicMock()
        client.resolve_all_variables.return_value = ["B24010_001E"]
        client._is_estimate_or_moe.return_value = True
        client._column_type.return_value = "numeric"
        client._sanitize_column_name.side_effect = lambda c: c.replace(" ", "_")

        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )

        ddl = generate_ddl(spec, client)
        assert '"state"' in ddl
        assert '"county"' in ddl
        assert '"tract"' in ddl

    def test_includes_scd2_metadata_columns(self):
        client = MagicMock()
        client.resolve_all_variables.return_value = ["B24010_001E"]
        client._is_estimate_or_moe.return_value = True
        client._column_type.return_value = "numeric"
        client._sanitize_column_name.side_effect = lambda c: c.replace(" ", "_")

        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )

        ddl = generate_ddl(spec, client)
        for col in ["ingested_at", "record_hash", "valid_from", "valid_to"]:
            assert col in ddl

    def test_unions_variables_across_vintages(self):
        """DDL should include variables from all vintages, not just one."""
        client = MagicMock()
        # Different variables per vintage
        client.resolve_all_variables.side_effect = [
            ["B24010_001E"],
            ["B24010_001E", "B24010_002E"],
        ]
        client._is_estimate_or_moe.return_value = True
        client._column_type.return_value = "numeric"
        client._sanitize_column_name.side_effect = lambda c: c.replace(" ", "_")

        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2021, 2022],
            groups=["B24010"],
            geography_level="tract",
            target_table="t",
            target_schema="raw_data",
        )

        ddl = generate_ddl(spec, client)
        assert "B24010_001E" in ddl
        assert "B24010_002E" in ddl
