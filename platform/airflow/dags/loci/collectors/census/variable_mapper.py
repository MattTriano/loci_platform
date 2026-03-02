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

import pandas as pd
from loci.collectors.census.abbreviations import ABBREVIATIONS
from loci.collectors.census.metadata import CensusMetadata
from loci.collectors.census.spec import CensusDatasetSpec

# Maximum PostgreSQL identifier length.
PG_MAX_IDENTIFIER = 63

# Variable suffixes we care about.  Annotations (EA, MA) are skipped.
_SUFFIX_MAP = {
    "E": "est",
    "M": "moe",
}

# Tokens to drop entirely (articles, prepositions that add no meaning).
_DROP_TOKENS = {"the", "of", "and", "or", "in", "to", "for", "by"}

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
        cleaned = re.sub(r"[^a-zA-Z0-9\s]", "", text).lower()
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

    def _deduplicate_consecutive(self, tokens: list[str]) -> list[str]:
        """Remove consecutive duplicate tokens from a list."""
        if not tokens:
            return []
        deduped = [tokens[0]]
        for t in tokens[1:]:
            if t != deduped[-1]:
                deduped.append(t)
        return deduped

    def _assemble_column_name(
        self,
        concept_prefix: str,
        label_compressed: str,
        suffix: str,
        variable_code: str,
    ) -> str:
        """
        Assemble the column name and truncate to stay within PG_MAX_IDENTIFIER.

        Format: {concept_prefix}__{label}_{est|moe}_{variable_code}

        The variable_code suffix is never truncated (it guarantees uniqueness).
        When the name is too long, the concept prefix is shortened first,
        then the label, both at underscore boundaries.
        """
        # The suffix portion is fixed and must not be truncated.
        fixed_tail = f"_{suffix}_{variable_code}"

        # Deduplicate concept and label tokens individually.
        concept_tokens = (
            self._deduplicate_consecutive(concept_prefix.split("_")) if concept_prefix else []
        )
        if label_compressed:
            label_tokens = self._deduplicate_consecutive(label_compressed.split("_"))
        else:
            label_tokens = ["tot"]

        # Budget for concept + label (plus the __ separator between them).
        # Full format: concept__label_{suffix}_{variable_code}
        separator = "__"
        budget = PG_MAX_IDENTIFIER - len(fixed_tail) - len(separator)

        concept_part = "_".join(concept_tokens)
        label_part = "_".join(label_tokens)

        # Trim concept first (at word boundaries), then label if still over.
        while len(concept_part) + len(label_part) > budget and concept_tokens:
            concept_tokens.pop()
            concept_part = "_".join(concept_tokens)

        while len(concept_part) + len(label_part) > budget and label_tokens:
            label_tokens.pop()
            label_part = "_".join(label_tokens)

        if concept_part and label_part:
            return f"{concept_part}{separator}{label_part}{fixed_tail}"
        elif label_part:
            return f"{label_part}{fixed_tail}"
        else:
            return f"{concept_part}{fixed_tail}"

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
