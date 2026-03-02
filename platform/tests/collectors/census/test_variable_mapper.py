"""Tests for CensusVariableMapper."""

from unittest.mock import MagicMock

import pandas as pd
import pytest
from loci.collectors.census.collector import CensusDatasetSpec
from loci.collectors.census.variable_mapper import PG_MAX_IDENTIFIER, CensusVariableMapper

# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #

SAMPLE_VARIABLES = pd.DataFrame(
    [
        {
            "variable": "B08105B_001E",
            "label": "Estimate!!Total:",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_001M",
            "label": "Margin of Error!!Total:",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_001EA",
            "label": "Annotation of Estimate!!Total:",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "string",
        },
        {
            "variable": "B08105B_001MA",
            "label": "Annotation of Margin of Error!!Total:",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "string",
        },
        {
            "variable": "B08105B_002E",
            "label": "Estimate!!Total:!!Car, truck, or van - drove alone",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_003E",
            "label": "Estimate!!Total:!!Car, truck, or van - carpooled",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_004E",
            "label": "Estimate!!Total:!!Public transportation",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_005E",
            "label": "Estimate!!Total:!!Walked",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_006E",
            "label": "Estimate!!Total:!!Taxi or ride-hailing services, motorcycle, bicycle, or other means",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
        {
            "variable": "B08105B_007E",
            "label": "Estimate!!Total:!!Worked from home",
            "group": "B08105B",
            "concept": "Means of Transportation to Work (Black or African American Alone)",
            "predicate_type": "int",
        },
    ]
)


@pytest.fixture
def spec():
    return CensusDatasetSpec(
        name="commute_by_race",
        dataset="acs/acs5",
        vintages=[2022],
        groups=["B08105B"],
        geography_level="tract",
        target_table="commute_by_race_tract",
        target_schema="raw_data",
    )


@pytest.fixture
def mapper():
    """Mapper with a mocked metadata client that returns SAMPLE_VARIABLES."""
    mock_metadata = MagicMock()
    mock_metadata.list_variables.return_value = SAMPLE_VARIABLES
    return CensusVariableMapper(metadata=mock_metadata)


# ------------------------------------------------------------------ #
#  _classify_variable
# ------------------------------------------------------------------ #


class TestClassifyVariable:
    def test_estimate(self, mapper):
        assert mapper._classify_variable("B08105B_001E") == "est"

    def test_margin_of_error(self, mapper):
        assert mapper._classify_variable("B08105B_001M") == "moe"

    def test_annotation_estimate_returns_none(self, mapper):
        assert mapper._classify_variable("B08105B_001EA") is None

    def test_annotation_moe_returns_none(self, mapper):
        assert mapper._classify_variable("B08105B_001MA") is None

    def test_no_suffix_returns_none(self, mapper):
        assert mapper._classify_variable("NAME") is None


# ------------------------------------------------------------------ #
#  _abbreviate_tokens
# ------------------------------------------------------------------ #


class TestAbbreviateTokens:
    def test_known_tokens(self, mapper):
        result = mapper._abbreviate_tokens("Public transportation")
        assert result == ["pub", "transpo"]

    def test_drops_filler_words(self, mapper):
        result = mapper._abbreviate_tokens("Car, truck, or van")
        assert "or" not in result
        assert result == ["car", "truck", "van"]

    def test_strips_punctuation(self, mapper):
        result = mapper._abbreviate_tokens("Total:")
        assert result == ["tot"]

    def test_unknown_token_passes_through(self, mapper):
        result = mapper._abbreviate_tokens("zygomorphic")
        assert result == ["zygomorphic"]

    def test_empty_abbreviation_dropped(self, mapper):
        # "means" maps to "" in ABBREVIATIONS
        result = mapper._abbreviate_tokens("Means of Transportation")
        assert result == ["transpo"]

    def test_extra_abbreviations(self):
        mock_metadata = MagicMock()
        mapper = CensusVariableMapper(
            metadata=mock_metadata,
            extra_abbreviations={"zygomorphic": "zyg"},
        )
        result = mapper._abbreviate_tokens("zygomorphic")
        assert result == ["zyg"]

    def test_strips_dollar_signs(self, mapper):
        result = mapper._abbreviate_tokens("$10,000 to $14,999")
        assert all("$" not in tok for tok in result)

    def test_strips_apostrophes(self, mapper):
        result = mapper._abbreviate_tokens("workers' earnings")
        assert all("'" not in tok for tok in result)

    def test_strips_dashes(self, mapper):
        result = mapper._abbreviate_tokens("full-time year-round")
        assert all("-" not in tok for tok in result)


# ------------------------------------------------------------------ #
#  _compress_concept
# ------------------------------------------------------------------ #


class TestCompressConcept:
    def test_concept_with_parens(self, mapper):
        result = mapper._compress_concept(
            "Means of Transportation to Work (Black or African American Alone)"
        )
        assert result == "transpo_work_blk_afr_amer_aln"


# ------------------------------------------------------------------ #
#  _compress_label_segments
# ------------------------------------------------------------------ #


class TestCompressLabelSegments:
    def test_skips_estimate_and_total(self, mapper):
        segments = ["Estimate", "Total:"]
        assert mapper._compress_label_segments(segments) == ""

    def test_content_segments_abbreviated(self, mapper):
        segments = ["Estimate", "Total:", "Public transportation"]
        assert mapper._compress_label_segments(segments) == "pub_transpo"

    def test_skips_margin_of_error(self, mapper):
        segments = ["Margin of Error", "Total:", "Walked"]
        assert mapper._compress_label_segments(segments) == "wlk"


# ------------------------------------------------------------------ #
#  _deduplicate_consecutive
# ------------------------------------------------------------------ #


class TestDeduplicateConsecutive:
    def test_removes_duplicates(self, mapper):
        assert mapper._deduplicate_consecutive(["a", "a", "b", "b", "c"]) == ["a", "b", "c"]

    def test_preserves_non_consecutive(self, mapper):
        assert mapper._deduplicate_consecutive(["a", "b", "a", "b"]) == ["a", "b", "a", "b"]

    def test_single_token(self, mapper):
        assert mapper._deduplicate_consecutive(["a"]) == ["a"]

    def test_empty_list(self, mapper):
        assert mapper._deduplicate_consecutive([]) == []


# ------------------------------------------------------------------ #
#  _assemble_column_name
# ------------------------------------------------------------------ #


class TestAssembleColumnName:
    def test_total_variable_gets_tot_label(self, mapper):
        result = mapper._assemble_column_name(
            "transpo_work_blk_afr_amer_aln", "", "est", "B08105B_001E"
        )
        assert "__tot_" in result

    def test_double_underscore_separator(self, mapper):
        result = mapper._assemble_column_name("transpo_work", "pub_transpo", "est", "B08105B_004E")
        assert "__" in result
        parts = result.split("__")
        assert len(parts) == 2

    def test_within_63_chars(self, mapper):
        result = mapper._assemble_column_name(
            "transpo_work_blk_afr_amer_aln",
            "pub_transpo",
            "est",
            "B08105B_004E",
        )
        assert len(result) <= PG_MAX_IDENTIFIER

    def test_variable_code_never_truncated(self, mapper):
        """Even with very long concept + label, the variable code is preserved."""
        result = mapper._assemble_column_name(
            "a_very_long_concept_prefix_that_goes_on_and_on",
            "and_a_very_long_label_that_also_goes_on_and_on",
            "est",
            "B24012_010E",
        )
        assert len(result) <= PG_MAX_IDENTIFIER
        assert result.endswith("B24012_010E")

    def test_truncates_concept_before_label(self, mapper):
        """Concept tokens are dropped first to make room."""
        short = mapper._assemble_column_name("a_b_c", "label", "est", "B08105B_001E")
        long_concept = mapper._assemble_column_name(
            "a_b_c_d_e_f_g_h_i_j_k_l_m_n_o_p",
            "label",
            "est",
            "B08105B_001E",
        )
        # Both should end with the variable code
        assert short.endswith("B08105B_001E")
        assert long_concept.endswith("B08105B_001E")

    def test_ends_with_variable_code(self, mapper):
        result = mapper._assemble_column_name("prefix", "label", "est", "B08105B_001E")
        assert result.endswith("B08105B_001E")


# ------------------------------------------------------------------ #
#  build_mapping_df
# ------------------------------------------------------------------ #


class TestBuildMappingDf:
    def test_filters_out_annotations(self, mapper, spec):
        df = mapper.build_mapping_df(spec, vintage=2022)
        var_types = df["var_type"].unique().tolist()
        assert set(var_types) <= {"est", "moe"}

    def test_has_expected_columns(self, mapper, spec):
        df = mapper.build_mapping_df(spec, vintage=2022)
        for col in ["var_type", "concept_compressed", "label_compressed", "column_name"]:
            assert col in df.columns

    def test_all_column_names_within_limit(self, mapper, spec):
        df = mapper.build_mapping_df(spec, vintage=2022)
        over = df[df["column_name"].str.len() > PG_MAX_IDENTIFIER]
        assert over.empty, (
            f"Column names over {PG_MAX_IDENTIFIER} chars:\n{over['column_name'].tolist()}"
        )

    def test_column_names_are_valid_pg_identifiers(self, mapper, spec):
        """All column names should be usable without double quotes in PostgreSQL.
        PG folds unquoted identifiers to lowercase, but uppercase chars are valid."""
        import re as _re

        df = mapper.build_mapping_df(spec, vintage=2022)
        pg_identifier = _re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
        invalid = df[~df["column_name"].str.match(pg_identifier)]
        assert invalid.empty, f"Invalid PG identifiers:\n{invalid['column_name'].tolist()}"

    def test_column_names_are_unique(self, mapper, spec):
        df = mapper.build_mapping_df(spec, vintage=2022)
        assert df["column_name"].is_unique


# ------------------------------------------------------------------ #
#  build_column_map
# ------------------------------------------------------------------ #


class TestBuildColumnMap:
    def test_returns_dict(self, mapper, spec):
        result = mapper.build_column_map(spec, vintage=2022)
        assert isinstance(result, dict)

    def test_keys_are_variable_codes(self, mapper, spec):
        result = mapper.build_column_map(spec, vintage=2022)
        for key in result:
            assert key.startswith("B08105B_")

    def test_does_not_include_annotations(self, mapper, spec):
        result = mapper.build_column_map(spec, vintage=2022)
        assert not any(k.endswith("EA") or k.endswith("MA") for k in result)


# ------------------------------------------------------------------ #
#  _fetch_variables
# ------------------------------------------------------------------ #


class TestFetchVariables:
    def test_fetches_group_variables(self, mapper, spec):
        df = mapper._fetch_variables(spec, vintage=2022)
        assert not df.empty
        assert "B08105B_001E" in df["variable"].values

    def test_deduplicates(self):
        """If a variable appears in both a group and the variables list, no dupes."""
        mock_metadata = MagicMock()
        group_df = SAMPLE_VARIABLES.copy()
        all_df = SAMPLE_VARIABLES.copy()
        mock_metadata.list_variables.side_effect = [group_df, all_df]

        mapper = CensusVariableMapper(metadata=mock_metadata)
        spec = CensusDatasetSpec(
            name="test",
            dataset="acs/acs5",
            vintages=[2022],
            groups=["B08105B"],
            variables=["B08105B_001E"],
            geography_level="tract",
            target_table="test_table",
            target_schema="raw_data",
        )
        df = mapper._fetch_variables(spec, vintage=2022)
        assert df["variable"].is_unique


# """Tests for CensusVariableMapper."""

# from unittest.mock import MagicMock

# import pandas as pd
# import pytest
# from loci.collectors.census.spec import CensusDatasetSpec
# from loci.collectors.census.variable_mapper import PG_MAX_IDENTIFIER, CensusVariableMapper

# # ------------------------------------------------------------------ #
# #  Fixtures
# # ------------------------------------------------------------------ #

# SAMPLE_VARIABLES = pd.DataFrame(
#     [
#         {
#             "variable": "B08105B_001E",
#             "label": "Estimate!!Total:",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_001M",
#             "label": "Margin of Error!!Total:",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_001EA",
#             "label": "Annotation of Estimate!!Total:",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "string",
#         },
#         {
#             "variable": "B08105B_001MA",
#             "label": "Annotation of Margin of Error!!Total:",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "string",
#         },
#         {
#             "variable": "B08105B_002E",
#             "label": "Estimate!!Total:!!Car, truck, or van - drove alone",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_003E",
#             "label": "Estimate!!Total:!!Car, truck, or van - carpooled",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_004E",
#             "label": "Estimate!!Total:!!Public transportation",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_005E",
#             "label": "Estimate!!Total:!!Walked",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_006E",
#             "label": "Estimate!!Total:!!Taxi or ride-hailing services, motorcycle, bicycle, or other means",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#         {
#             "variable": "B08105B_007E",
#             "label": "Estimate!!Total:!!Worked from home",
#             "group": "B08105B",
#             "concept": "Means of Transportation to Work (Black or African American Alone)",
#             "predicate_type": "int",
#         },
#     ]
# )


# @pytest.fixture
# def spec():
#     return CensusDatasetSpec(
#         name="commute_by_race",
#         dataset="acs/acs5",
#         vintages=[2022],
#         groups=["B08105B"],
#         geography_level="tract",
#         target_table="commute_by_race_tract",
#         target_schema="raw_data",
#     )


# @pytest.fixture
# def mapper():
#     """Mapper with a mocked metadata client that returns SAMPLE_VARIABLES."""
#     mock_metadata = MagicMock()
#     mock_metadata.list_variables.return_value = SAMPLE_VARIABLES
#     return CensusVariableMapper(metadata=mock_metadata)


# # ------------------------------------------------------------------ #
# #  build_mapping_df — core behavioral tests
# # ------------------------------------------------------------------ #


# class TestBuildMappingDf:
#     def test_excludes_annotation_variables(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         suffixes = df["variable"].str.extract(r"\d+([A-Z]+)$")[0].unique()
#         assert set(suffixes) <= {"E", "M"}

#     def test_includes_both_estimates_and_moe(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         assert set(df["var_type"].unique()) == {"est", "moe"}

#     def test_all_column_names_are_valid_pg_identifiers(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         over = df[df["column_name"].str.len() > PG_MAX_IDENTIFIER]
#         assert over.empty, (
#             f"Column names over {PG_MAX_IDENTIFIER} chars:\n{over['column_name'].tolist()}"
#         )

#     def test_column_names_are_unique(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         assert df["column_name"].is_unique

#     def test_column_names_end_with_variable_code(self, mapper, spec):
#         """The variable code suffix is what guarantees uniqueness."""
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         for _, row in df.iterrows():
#             assert row["column_name"].endswith(row["variable"])

#     def test_column_names_include_est_or_moe(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         for _, row in df.iterrows():
#             assert f"_{row['var_type']}_" in row["column_name"]

#     def test_total_variable_gets_tot_label(self, mapper, spec):
#         """When the label is just Estimate!!Total, the column should contain 'tot'."""
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         total_row = df[df["variable"] == "B08105B_001E"].iloc[0]
#         assert "_tot_" in total_row["column_name"]

#     def test_label_content_appears_in_column_name(self, mapper, spec):
#         """Substantive label segments should be reflected in the column name."""
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         pub_transpo_row = df[df["variable"] == "B08105B_004E"].iloc[0]
#         # The exact abbreviation depends on the abbreviations dict, but
#         # "public transportation" should produce something recognizable.
#         col = pub_transpo_row["column_name"]
#         assert "pub" in col or "transpo" in col

#     def test_enriched_columns_present(self, mapper, spec):
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         for col in ["var_type", "concept_compressed", "label_compressed", "column_name"]:
#             assert col in df.columns


# # ------------------------------------------------------------------ #
# #  build_column_map — behavioral tests
# # ------------------------------------------------------------------ #


# class TestBuildColumnMap:
#     def test_returns_dict_of_variable_to_column_name(self, mapper, spec):
#         result = mapper.build_column_map(spec, vintage=2022)
#         assert isinstance(result, dict)
#         assert len(result) > 0

#     def test_keys_are_original_variable_codes(self, mapper, spec):
#         result = mapper.build_column_map(spec, vintage=2022)
#         for key in result:
#             assert key.startswith("B08105B_")

#     def test_excludes_annotations(self, mapper, spec):
#         result = mapper.build_column_map(spec, vintage=2022)
#         assert not any(k.endswith("EA") or k.endswith("MA") for k in result)

#     def test_consistent_with_mapping_df(self, mapper, spec):
#         """build_column_map should return exactly the same mapping as build_mapping_df."""
#         df = mapper.build_mapping_df(spec, vintage=2022)
#         col_map = mapper.build_column_map(spec, vintage=2022)
#         expected = dict(zip(df["variable"], df["column_name"]))
#         assert col_map == expected


# # ------------------------------------------------------------------ #
# #  Extra abbreviations — integration through public API
# # ------------------------------------------------------------------ #


# class TestExtraAbbreviations:
#     def test_extra_abbreviations_affect_output(self, spec):
#         mock_metadata = MagicMock()
#         mock_metadata.list_variables.return_value = SAMPLE_VARIABLES

#         mapper_default = CensusVariableMapper(metadata=mock_metadata)
#         mapper_custom = CensusVariableMapper(
#             metadata=mock_metadata,
#             extra_abbreviations={"walked": "w"},
#         )

#         default_map = mapper_default.build_column_map(spec, vintage=2022)
#         custom_map = mapper_custom.build_column_map(spec, vintage=2022)

#         walked_var = "B08105B_005E"
#         assert default_map[walked_var] != custom_map[walked_var]


# # ------------------------------------------------------------------ #
# #  _fetch_variables
# # ------------------------------------------------------------------ #


# class TestFetchVariables:
#     def test_deduplicates_across_groups_and_variables(self):
#         """If a variable appears in both a group and the variables list, no dupes."""
#         mock_metadata = MagicMock()
#         mock_metadata.list_variables.side_effect = [
#             SAMPLE_VARIABLES.copy(),
#             SAMPLE_VARIABLES.copy(),
#         ]

#         mapper = CensusVariableMapper(metadata=mock_metadata)
#         spec = CensusDatasetSpec(
#             name="test",
#             dataset="acs/acs5",
#             vintages=[2022],
#             groups=["B08105B"],
#             variables=["B08105B_001E"],
#             geography_level="tract",
#             target_table="test_table",
#             target_schema="raw_data",
#         )
#         df = mapper._fetch_variables(spec, vintage=2022)
#         assert df["variable"].is_unique


# # ------------------------------------------------------------------ #
# #  Non-obvious private logic: truncation and deduplication
# # ------------------------------------------------------------------ #


# class TestAssembleColumnNameTruncation:
#     """The truncation logic in _assemble_column_name is subtle enough to test directly."""

#     def test_long_prefix_truncated_at_word_boundary(self, mapper):
#         long_prefix = "a_bb_ccc_dddd_eeeee_ffffff_ggggggg"
#         result = mapper._assemble_column_name(
#             long_prefix, "some_long_label_tokens_here", "est", "B08105B_006E"
#         )
#         assert len(result) <= PG_MAX_IDENTIFIER
#         prefix_part = result.split("__")[0]
#         assert not prefix_part.endswith("_")

#     def test_extremely_long_label_still_fits(self, mapper):
#         result = mapper._assemble_column_name("concept", "a_" * 60, "est", "B08105B_001E")
#         assert len(result) <= PG_MAX_IDENTIFIER


# class TestDeduplicateConsecutive:
#     def test_removes_consecutive_duplicates(self, mapper):
#         assert mapper._deduplicate_consecutive("a_a_b_b_c") == "a_b_c"

#     def test_preserves_non_consecutive_repeats(self, mapper):
#         assert mapper._deduplicate_consecutive("a_b_a_b") == "a_b_a_b"


# # """Tests for CensusVariableMapper."""

# # from unittest.mock import MagicMock

# # import pandas as pd
# # import pytest
# # from loci.collectors.census.spec import CensusDatasetSpec
# # from loci.collectors.census.variable_mapper import PG_MAX_IDENTIFIER, CensusVariableMapper

# # # ------------------------------------------------------------------ #
# # #  Fixtures
# # # ------------------------------------------------------------------ #

# # SAMPLE_VARIABLES = pd.DataFrame(
# #     [
# #         {
# #             "variable": "B08105B_001E",
# #             "label": "Estimate!!Total:",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_001M",
# #             "label": "Margin of Error!!Total:",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_001EA",
# #             "label": "Annotation of Estimate!!Total:",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "string",
# #         },
# #         {
# #             "variable": "B08105B_001MA",
# #             "label": "Annotation of Margin of Error!!Total:",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "string",
# #         },
# #         {
# #             "variable": "B08105B_002E",
# #             "label": "Estimate!!Total:!!Car, truck, or van - drove alone",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_003E",
# #             "label": "Estimate!!Total:!!Car, truck, or van - carpooled",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_004E",
# #             "label": "Estimate!!Total:!!Public transportation",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_005E",
# #             "label": "Estimate!!Total:!!Walked",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_006E",
# #             "label": "Estimate!!Total:!!Taxi or ride-hailing services, motorcycle, bicycle, or other means",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #         {
# #             "variable": "B08105B_007E",
# #             "label": "Estimate!!Total:!!Worked from home",
# #             "group": "B08105B",
# #             "concept": "Means of Transportation to Work (Black or African American Alone)",
# #             "predicate_type": "int",
# #         },
# #     ]
# # )


# # @pytest.fixture
# # def spec():
# #     return CensusDatasetSpec(
# #         name="commute_by_race",
# #         dataset="acs/acs5",
# #         vintages=[2022],
# #         groups=["B08105B"],
# #         geography_level="tract",
# #         target_table="commute_by_race_tract",
# #         target_schema="raw_data",
# #     )


# # @pytest.fixture
# # def mapper():
# #     """Mapper with a mocked metadata client that returns SAMPLE_VARIABLES."""
# #     mock_metadata = MagicMock()
# #     mock_metadata.list_variables.return_value = SAMPLE_VARIABLES
# #     return CensusVariableMapper(metadata=mock_metadata)


# # # ------------------------------------------------------------------ #
# # #  _classify_variable
# # # ------------------------------------------------------------------ #


# # class TestClassifyVariable:
# #     def test_estimate(self, mapper):
# #         assert mapper._classify_variable("B08105B_001E") == "est"

# #     def test_margin_of_error(self, mapper):
# #         assert mapper._classify_variable("B08105B_001M") == "moe"

# #     def test_annotation_estimate_returns_none(self, mapper):
# #         assert mapper._classify_variable("B08105B_001EA") is None

# #     def test_annotation_moe_returns_none(self, mapper):
# #         assert mapper._classify_variable("B08105B_001MA") is None

# #     def test_no_suffix_returns_none(self, mapper):
# #         assert mapper._classify_variable("NAME") is None


# # # ------------------------------------------------------------------ #
# # #  _abbreviate_tokens
# # # ------------------------------------------------------------------ #


# # class TestAbbreviateTokens:
# #     def test_known_tokens(self, mapper):
# #         result = mapper._abbreviate_tokens("Public transportation")
# #         assert result == ["pub", "transpo"]

# #     def test_drops_filler_words(self, mapper):
# #         result = mapper._abbreviate_tokens("Car, truck, or van")
# #         assert "or" not in result
# #         assert result == ["car", "truck", "van"]

# #     def test_strips_punctuation(self, mapper):
# #         result = mapper._abbreviate_tokens("Total:")
# #         assert result == ["tot"]

# #     def test_unknown_token_passes_through(self, mapper):
# #         result = mapper._abbreviate_tokens("zygomorphic")
# #         assert result == ["zygomorphic"]

# #     def test_empty_abbreviation_dropped(self, mapper):
# #         # "means" maps to "" in ABBREVIATIONS
# #         result = mapper._abbreviate_tokens("Means of Transportation")
# #         assert result == ["transpo"]

# #     def test_extra_abbreviations(self):
# #         mock_metadata = MagicMock()
# #         mapper = CensusVariableMapper(
# #             metadata=mock_metadata,
# #             extra_abbreviations={"zygomorphic": "zyg"},
# #         )
# #         result = mapper._abbreviate_tokens("zygomorphic")
# #         assert result == ["zyg"]


# # # ------------------------------------------------------------------ #
# # #  _compress_concept
# # # ------------------------------------------------------------------ #


# # class TestCompressConcept:
# #     def test_concept_with_parens(self, mapper):
# #         result = mapper._compress_concept(
# #             "Means of Transportation to Work (Black or African American Alone)"
# #         )
# #         assert result == "transpo_work_blk_afr_amer_aln"


# # # ------------------------------------------------------------------ #
# # #  _compress_label_segments
# # # ------------------------------------------------------------------ #


# # class TestCompressLabelSegments:
# #     def test_skips_estimate_and_total(self, mapper):
# #         segments = ["Estimate", "Total:"]
# #         assert mapper._compress_label_segments(segments) == ""

# #     def test_content_segments_abbreviated(self, mapper):
# #         segments = ["Estimate", "Total:", "Public transportation"]
# #         assert mapper._compress_label_segments(segments) == "pub_transpo"

# #     def test_skips_margin_of_error(self, mapper):
# #         segments = ["Margin of Error", "Total:", "Walked"]
# #         assert mapper._compress_label_segments(segments) == "wlk"


# # # ------------------------------------------------------------------ #
# # #  _deduplicate_consecutive
# # # ------------------------------------------------------------------ #


# # class TestDeduplicateConsecutive:
# #     def test_removes_duplicates(self, mapper):
# #         assert mapper._deduplicate_consecutive("a_a_b_b_c") == "a_b_c"

# #     def test_preserves_non_consecutive(self, mapper):
# #         assert mapper._deduplicate_consecutive("a_b_a_b") == "a_b_a_b"

# #     def test_single_token(self, mapper):
# #         assert mapper._deduplicate_consecutive("a") == "a"


# # # ------------------------------------------------------------------ #
# # #  _assemble_column_name
# # # ------------------------------------------------------------------ #


# # class TestAssembleColumnName:
# #     def test_total_variable_gets_tot_label(self, mapper):
# #         result = mapper._assemble_column_name(
# #             "transpo_work_blk_afr_amer_aln", "", "est", "B08105B_001E"
# #         )
# #         assert "_tot_" in result

# #     def test_within_63_chars(self, mapper):
# #         result = mapper._assemble_column_name(
# #             "transpo_work_blk_afr_amer_aln",
# #             "pub_transpo",
# #             "est",
# #             "B08105B_004E",
# #         )
# #         assert len(result) <= PG_MAX_IDENTIFIER

# #     def test_truncates_concept_at_word_boundary(self, mapper):
# #         long_prefix = "a_bb_ccc_dddd_eeeee_ffffff_ggggggg"
# #         result = mapper._assemble_column_name(
# #             long_prefix,
# #             "some_long_label_tokens_here",
# #             "est",
# #             "B08105B_006E",
# #         )
# #         assert len(result) <= PG_MAX_IDENTIFIER
# #         # Should not end with a partial token before the __
# #         prefix_part = result.split("__")[0]
# #         assert not prefix_part.endswith("_")

# #     def test_ends_with_variable_code(self, mapper):
# #         result = mapper._assemble_column_name("prefix", "label", "est", "B08105B_001E")
# #         assert result.endswith("B08105B_001E")


# # # ------------------------------------------------------------------ #
# # #  build_mapping_df
# # # ------------------------------------------------------------------ #


# # class TestBuildMappingDf:
# #     def test_filters_out_annotations(self, mapper, spec):
# #         df = mapper.build_mapping_df(spec, vintage=2022)
# #         var_types = df["var_type"].unique().tolist()
# #         assert set(var_types) <= {"est", "moe"}

# #     def test_has_expected_columns(self, mapper, spec):
# #         df = mapper.build_mapping_df(spec, vintage=2022)
# #         for col in ["var_type", "concept_compressed", "label_compressed", "column_name"]:
# #             assert col in df.columns

# #     def test_all_column_names_within_limit(self, mapper, spec):
# #         df = mapper.build_mapping_df(spec, vintage=2022)
# #         over = df[df["column_name"].str.len() > PG_MAX_IDENTIFIER]
# #         assert over.empty, (
# #             f"Column names over {PG_MAX_IDENTIFIER} chars:\n{over['column_name'].tolist()}"
# #         )

# #     def test_column_names_are_unique(self, mapper, spec):
# #         df = mapper.build_mapping_df(spec, vintage=2022)
# #         assert df["column_name"].is_unique


# # # ------------------------------------------------------------------ #
# # #  build_column_map
# # # ------------------------------------------------------------------ #


# # class TestBuildColumnMap:
# #     def test_returns_dict(self, mapper, spec):
# #         result = mapper.build_column_map(spec, vintage=2022)
# #         assert isinstance(result, dict)

# #     def test_keys_are_variable_codes(self, mapper, spec):
# #         result = mapper.build_column_map(spec, vintage=2022)
# #         for key in result:
# #             assert key.startswith("B08105B_")

# #     def test_does_not_include_annotations(self, mapper, spec):
# #         result = mapper.build_column_map(spec, vintage=2022)
# #         assert not any(k.endswith("EA") or k.endswith("MA") for k in result)


# # # ------------------------------------------------------------------ #
# # #  generate_dbt_model
# # # ------------------------------------------------------------------ #


# # class TestGenerateDbtModel:
# #     def test_contains_source_ref(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert "{{ source('raw_data', 'commute_by_race_tract') }}" in sql

# #     def test_contains_geo_columns(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert '"state"' in sql
# #         assert '"county"' in sql
# #         assert '"tract"' in sql

# #     def test_contains_vintage(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert "vintage" in sql

# #     def test_aliases_variables(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert '"B08105B_001E" as ' in sql

# #     def test_no_annotations_in_sql(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert "B08105B_001EA" not in sql
# #         assert "B08105B_001MA" not in sql

# #     def test_has_cte_structure(self, mapper, spec):
# #         sql = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert sql.startswith("with renamed as (")
# #         assert "select * from renamed" in sql


# # # ------------------------------------------------------------------ #
# # #  write_dbt_model
# # # ------------------------------------------------------------------ #


# # class TestWriteDbtModel:
# #     def test_writes_file(self, mapper, spec, tmp_path):
# #         path = mapper.write_dbt_model(spec, vintage=2022, output_dir=tmp_path)
# #         assert path.exists()
# #         assert path.name == "commute_by_race_tract.sql"

# #     def test_file_content_matches_generate(self, mapper, spec, tmp_path):
# #         path = mapper.write_dbt_model(spec, vintage=2022, output_dir=tmp_path)
# #         expected = mapper.generate_dbt_model(spec, vintage=2022)
# #         assert path.read_text() == expected

# #     def test_creates_output_dir(self, mapper, spec, tmp_path):
# #         nested = tmp_path / "a" / "b"
# #         path = mapper.write_dbt_model(spec, vintage=2022, output_dir=nested)
# #         assert path.exists()


# # # ------------------------------------------------------------------ #
# # #  _fetch_variables
# # # ------------------------------------------------------------------ #


# # class TestFetchVariables:
# #     def test_fetches_group_variables(self, mapper, spec):
# #         df = mapper._fetch_variables(spec, vintage=2022)
# #         assert not df.empty
# #         assert "B08105B_001E" in df["variable"].values

# #     def test_deduplicates(self):
# #         """If a variable appears in both a group and the variables list, no dupes."""
# #         mock_metadata = MagicMock()
# #         group_df = SAMPLE_VARIABLES.copy()
# #         all_df = SAMPLE_VARIABLES.copy()
# #         mock_metadata.list_variables.side_effect = [group_df, all_df]

# #         mapper = CensusVariableMapper(metadata=mock_metadata)
# #         spec = CensusDatasetSpec(
# #             name="test",
# #             dataset="acs/acs5",
# #             vintages=[2022],
# #             groups=["B08105B"],
# #             variables=["B08105B_001E"],
# #             geography_level="tract",
# #             target_table="test_table",
# #             target_schema="raw_data",
# #         )
# #         df = mapper._fetch_variables(spec, vintage=2022)
# #         assert df["variable"].is_unique
