"""Tests for DbtModelGenerator and CensusDbtPipelineBuilder."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from loci.collectors.census.spec import CensusDatasetSpec
from loci.transform.model_generator import (
    CensusDbtPipelineBuilder,
    DbtModelGenerator,
)

# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #


@pytest.fixture
def dbt_project(tmp_path):
    """A minimal dbt project directory with a sources.yml."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    sources_data = {
        "version": 2,
        "sources": [
            {
                "name": "raw_data",
                "schema": "raw_data",
                "tables": [
                    {"name": "existing_table"},
                ],
            }
        ],
    }
    (models_dir / "sources.yml").write_text(
        yaml.dump(sources_data, default_flow_style=False, sort_keys=False)
    )
    return tmp_path


@pytest.fixture
def generator(dbt_project):
    return DbtModelGenerator(dbt_project)


@pytest.fixture
def census_spec():
    return CensusDatasetSpec(
        name="commute_by_race",
        dataset="acs/acs5",
        vintages=[2022],
        groups=["B08105B"],
        geography_level="tract",
        target_table="commute_by_race_tract",
        target_schema="raw_data",
    )


# ------------------------------------------------------------------ #
#  build_loading_sql
# ------------------------------------------------------------------ #


class TestBuildLoadingSql:
    def test_references_source(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["col_a"],
        )
        assert "{{ source('raw_data', 'my_table') }}" in sql

    def test_aliases_columns_with_standardize_macro(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["B24010_001E"],
        )
        assert '{{ standardize_column_name("B24010_001E") }}' in sql

    def test_explicit_mapping_overrides_macro(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["B24010_001E"],
            column_mapping={"B24010_001E": "occupation_total_est"},
        )
        assert '"B24010_001E" as occupation_total_est' in sql
        assert "standardize_column_name" not in sql

    def test_scd2_adds_valid_to_filter(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["col_a"],
            is_scd2=True,
        )
        assert "valid_to is null" in sql.lower()

    def test_non_scd2_has_no_valid_to_filter(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["col_a"],
            is_scd2=False,
        )
        assert "valid_to" not in sql

    def test_passthrough_columns_appear_unaliased(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["B24010_001E"],
            passthrough_columns=["state", "county", "tract", "vintage"],
        )
        # Passthrough columns should appear as bare quoted identifiers
        assert '"state"' in sql
        assert '"vintage"' in sql
        # They should not be aliased
        assert '"state" as' not in sql

    def test_sql_is_valid_cte_structure(self, generator):
        sql = generator.build_loading_sql(
            source_name="raw_data",
            table_name="my_table",
            columns=["col_a"],
        )
        assert sql.strip().startswith("with ")
        assert "select *" in sql.lower()


# ------------------------------------------------------------------ #
#  generate_loading_model — file writing
# ------------------------------------------------------------------ #


class TestGenerateLoadingModel:
    def test_writes_sql_file(self, generator):
        path = generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        assert path.exists()
        assert path.suffix == ".sql"
        assert path.name == "stg__existing_table.sql"

    def test_file_content_matches_build_loading_sql(self, generator):
        expected = generator.build_loading_sql(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        path = generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        assert path.read_text() == expected

    def test_creates_staging_subdirectory(self, generator):
        path = generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        assert "staging" in path.parts
        assert "raw_data" in path.parts

    def test_refuses_overwrite_by_default(self, generator):
        generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        with pytest.raises(FileExistsError):
            generator.generate_loading_model(
                source_name="raw_data",
                table_name="existing_table",
                columns=["col_a"],
            )

    def test_overwrite_flag_replaces_file(self, generator):
        generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        path = generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_b"],
            overwrite=True,
        )
        content = path.read_text()
        assert "col_b" in content


# ------------------------------------------------------------------ #
#  _ensure_source_table — sources.yml management
# ------------------------------------------------------------------ #


class TestEnsureSourceTable:
    def test_adds_new_table_to_existing_source(self, generator):
        generator.generate_loading_model(
            source_name="raw_data",
            table_name="new_table",
            columns=["col_a"],
        )
        sources = yaml.safe_load(generator.sources_path.read_text())
        table_names = [t["name"] for t in sources["sources"][0]["tables"]]
        assert "new_table" in table_names

    def test_does_not_duplicate_existing_table(self, generator):
        generator.generate_loading_model(
            source_name="raw_data",
            table_name="existing_table",
            columns=["col_a"],
        )
        sources = yaml.safe_load(generator.sources_path.read_text())
        table_names = [t["name"] for t in sources["sources"][0]["tables"]]
        assert table_names.count("existing_table") == 1

    def test_raises_for_unknown_source(self, generator):
        with pytest.raises(ValueError, match="not found"):
            generator.generate_loading_model(
                source_name="nonexistent_source",
                table_name="some_table",
                columns=["col_a"],
            )


# ------------------------------------------------------------------ #
#  CensusDbtPipelineBuilder
# ------------------------------------------------------------------ #


class TestCensusDbtPipelineBuilder:
    def test_generates_model_with_compressed_column_names(self, dbt_project, census_spec):
        mock_generator = MagicMock()
        mock_generator.generate_loading_model.return_value = Path("/fake/path.sql")

        builder = CensusDbtPipelineBuilder(
            generator=mock_generator,
            metadata=MagicMock(),
        )
        builder._mapper = MagicMock()
        builder._mapper.build_column_map.return_value = {
            "B08105B_001E": "some_col_est",
            "B08105B_001M": "some_col_moe",
        }

        builder.generate_loading_model(spec=census_spec, vintage=2022)

        mock_generator.generate_loading_model.assert_called_once()
        call_kwargs = mock_generator.generate_loading_model.call_args.kwargs

        # Should be SCD2 (Census data uses SCD2 ingestion)
        assert call_kwargs["is_scd2"] is True
        # Should pass through geography + vintage columns
        assert "state" in call_kwargs["passthrough_columns"]
        assert "vintage" in call_kwargs["passthrough_columns"]
        # Source name and table come from the spec
        assert call_kwargs["source_name"] == "raw_data"
        assert call_kwargs["table_name"] == "commute_by_race_tract"

    def test_column_mapping_comes_from_variable_mapper(self, dbt_project, census_spec):
        """The column_mapping passed to the generator should be the mapper's output."""
        mock_metadata = MagicMock()

        mock_generator = MagicMock()
        mock_generator.generate_loading_model.return_value = Path("/fake/path.sql")

        builder = CensusDbtPipelineBuilder(
            generator=mock_generator,
            metadata=mock_metadata,
        )

        # Patch the mapper to return a known mapping
        builder._mapper = MagicMock()
        builder._mapper.build_column_map.return_value = {
            "B08105B_001E": "transpo_work_tot_est_B08105B_001E",
        }

        builder.generate_loading_model(spec=census_spec, vintage=2022)

        call_kwargs = mock_generator.generate_loading_model.call_args.kwargs
        assert call_kwargs["column_mapping"] == {
            "B08105B_001E": "transpo_work_tot_est_B08105B_001E",
        }
        assert call_kwargs["columns"] == ["B08105B_001E"]
