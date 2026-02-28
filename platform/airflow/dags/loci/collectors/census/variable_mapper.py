"""
CensusVariableMapper — compresses Census Bureau variable labels into
short, interpretable PostgreSQL column names (≤63 characters).

Output format:
    {concept_prefix}__{label_tokens}_{est|moe}_{variable_code}

The variable_code suffix guarantees uniqueness; the prefix and label
tokens provide human readability.

Usage:
    from census_metadata import CensusMetadata
    from census_collector import CensusDatasetSpec
    from variable_mapper import CensusVariableMapper

    cm = CensusMetadata(api_key="YOUR_KEY")
    spec = CensusDatasetSpec(
        name="commute_by_race",
        dataset="acs/acs5",
        vintages=[2022],
        groups=["B08105B"],
        geography_level="tract",
        target_table="commute_by_race_tract",
        target_schema="raw_data",
    )

    mapper = CensusVariableMapper(metadata=cm)
    enriched = mapper.build_mapping_df(spec, vintage=2022)
    name_map  = mapper.build_column_map(spec, vintage=2022)
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from loci.collectors.census.abbreviations import ABBREVIATIONS
from loci.collectors.census.metadata import CensusMetadata
from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# Maximum PostgreSQL identifier length.
PG_MAX_IDENTIFIER = 63

# Variable suffixes we care about.  Annotations (EA, MA) are skipped.
_SUFFIX_MAP = {
    "E": "est",
    "M": "moe",
}

# Tokens to drop entirely (articles, prepositions that add no meaning).
_DROP_TOKENS = {"the", "of", "and", "or", "in", "to", "for", "by", "-"}

# Label segments that carry no distinguishing information.
_STRUCTURAL_SEGMENTS = {
    "estimate",
    "margin of error",
    "total",
    "annotation of estimate",
    "annotation of margin of error",
}


class CensusVariableMapper:
    """
    Compresses Census variable labels into readable PostgreSQL column names.

    Parameters
    ----------
    metadata : CensusMetadata
        Used to fetch variable labels and concepts from the Census API.
    extra_abbreviations : dict[str, str], optional
        Additional abbreviations to merge with the default dictionary.
    """

    def __init__(
        self,
        metadata: CensusMetadata,
        extra_abbreviations: dict[str, str] | None = None,
    ):
        self._metadata = metadata
        self._abbreviations = dict(ABBREVIATIONS)
        if extra_abbreviations:
            self._abbreviations.update(extra_abbreviations)

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _classify_variable(self, variable: str) -> str | None:
        """Return 'est', 'moe', or None (for annotations / unrecognized)."""
        m = re.search(r"\d+([A-Z]+)$", variable)
        if not m:
            return None
        return _SUFFIX_MAP.get(m.group(1))

    def _abbreviate_tokens(self, text: str) -> list[str]:
        """
        Tokenize text, abbreviate each token via the dictionary,
        and drop filler words.

        Returns a list of abbreviated, lowercased tokens.
        """
        cleaned = re.sub(r"[():,]", "", text).lower()
        tokens = cleaned.split()
        result = []
        for tok in tokens:
            if tok in _DROP_TOKENS:
                continue
            abbreviated = self._abbreviations.get(tok, tok)
            if abbreviated == "":
                continue
            result.append(abbreviated)
        return result

    def _compress_concept(self, concept: str) -> str:
        """Abbreviate and join concept tokens into a short prefix."""
        tokens = self._abbreviate_tokens(concept)
        return "_".join(tokens)

    def _compress_label_segments(self, segments: list[str]) -> str:
        """
        Abbreviate each label segment (after splitting on '!!'),
        skipping structural segments (Estimate, Margin of Error, Total).

        Returns underscore-joined abbreviated tokens.
        """
        tokens = []
        for seg in segments:
            seg_clean = seg.strip().rstrip(":")
            if seg_clean.lower() in _STRUCTURAL_SEGMENTS:
                continue
            tokens.extend(self._abbreviate_tokens(seg_clean))
        return "_".join(tokens)

    def _deduplicate_consecutive(self, tokens_str: str) -> str:
        """Remove consecutive duplicate tokens in an underscore-joined string."""
        parts = tokens_str.split("_")
        deduped = [parts[0]] if parts else []
        for p in parts[1:]:
            if p != deduped[-1]:
                deduped.append(p)
        return "_".join(deduped)

    def _assemble_column_name(
        self,
        concept_prefix: str,
        label_compressed: str,
        suffix: str,
        variable_code: str,
    ) -> str:
        """
        Assemble the column name and truncate the concept prefix if needed
        to stay within PG_MAX_IDENTIFIER.

        Format: {concept_prefix}__{label}_{est|moe}_{variable_code}
        """
        if label_compressed:
            tail = f"__{label_compressed}_{suffix}_{variable_code}"
        else:
            tail = f"__tot_{suffix}_{variable_code}"

        tail = self._deduplicate_consecutive(tail)
        budget = PG_MAX_IDENTIFIER - len(tail)

        if budget <= 0:
            return tail.lstrip("_")[:PG_MAX_IDENTIFIER]

        if len(concept_prefix) > budget:
            prefix = concept_prefix[:budget]
            last_sep = prefix.rfind("_")
            prefix = prefix[:last_sep] if last_sep > 0 else prefix
        else:
            prefix = concept_prefix
        prefix = prefix.rstrip("_")

        full = f"{prefix}{tail}"
        full = self._deduplicate_consecutive(full)
        return full[:PG_MAX_IDENTIFIER]

    def _fetch_variables(self, spec: CensusDatasetSpec, vintage: int) -> pd.DataFrame:
        """
        Fetch variable metadata for all groups and individual variables
        defined in the spec.
        """
        frames = []
        for group in spec.groups:
            df = self._metadata.list_variables(spec.dataset, vintage, group=group)
            frames.append(df)
        if spec.variables:
            all_vars = self._metadata.list_variables(spec.dataset, vintage)
            frames.append(all_vars[all_vars["variable"].isin(spec.variables)])
        if not frames:
            return pd.DataFrame(columns=["variable", "label", "group", "concept", "predicate_type"])
        return pd.concat(frames, ignore_index=True).drop_duplicates(subset="variable")

    # ------------------------------------------------------------------ #
    #  Public methods
    # ------------------------------------------------------------------ #

    def build_mapping_df(self, spec: CensusDatasetSpec, vintage: int) -> pd.DataFrame:
        """
        Fetch variable metadata for a spec and return it enriched with:
          - var_type: 'est' or 'moe'
          - concept_compressed: abbreviated concept string
          - label_compressed: abbreviated label string (without concept/type prefix)
          - column_name: the final compressed column name

        Rows with annotations (EA, MA) or unrecognized suffixes are excluded.
        """
        variables_df = self._fetch_variables(spec, vintage)
        df = variables_df.copy()

        df["var_type"] = df["variable"].apply(self._classify_variable)
        df = df[df["var_type"].notna()].copy()

        df["concept_compressed"] = df["concept"].apply(self._compress_concept)

        df["label_compressed"] = df["label"].apply(
            lambda lbl: self._compress_label_segments(lbl.split("!!"))
        )

        df["column_name"] = df.apply(
            lambda row: self._assemble_column_name(
                row["concept_compressed"],
                row["label_compressed"],
                row["var_type"],
                row["variable"],
            ),
            axis=1,
        )

        return df.reset_index(drop=True)

    def build_column_map(self, spec: CensusDatasetSpec, vintage: int) -> dict[str, str]:
        """
        Return a dict mapping original variable codes to compressed column names.

        {variable_code: column_name}

        Suitable for use in a dbt model's column aliasing.
        """
        df = self.build_mapping_df(spec, vintage)
        return dict(zip(df["variable"], df["column_name"]))

    def generate_dbt_model(self, spec: CensusDatasetSpec, vintage: int) -> str:
        """
        Generate a dbt SQL model that aliases census variable columns to
        compressed names in a CTE.

        The model selects from {{ source(target_schema, target_table) }},
        passes through geography ID columns, and aliases each variable.
        """
        column_map = self.build_column_map(spec, vintage)
        geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]

        select_lines = []
        for col in geo_columns:
            select_lines.append(f'        "{col}"')
        select_lines.append("        vintage")
        for var_code, col_name in column_map.items():
            select_lines.append(f'        "{var_code}" as {col_name}')

        select_clause = ",\n".join(select_lines)
        source_ref = f"{{{{ source('{spec.target_schema}', '{spec.target_table}') }}}}"

        return (
            f"with aliased as (\n"
            f"    select\n"
            f"{select_clause}\n"
            f"    from {source_ref}\n"
            f")\n"
            f"\n"
            f"select * from aliased\n"
        )

    def write_dbt_model(
        self,
        spec: CensusDatasetSpec,
        vintage: int,
        output_dir: str | Path,
    ) -> Path:
        """
        Generate a dbt model SQL file and write it to output_dir.

        The file is named after the spec's target_table.

        Returns the path to the written file.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        sql = self.generate_dbt_model(spec, vintage)
        path = output_dir / f"{spec.target_table}.sql"
        path.write_text(sql)
        return path
