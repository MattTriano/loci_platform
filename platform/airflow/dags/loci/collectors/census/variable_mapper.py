# # # # """
# # # # CensusVariableMapper — maps Census variable codes to human-readable column names,
# # # # and generates dbt staging models that apply those mappings.

# # # # Column name format:
# # # #     {compressed_label}_{est|moe}_{variable_code}

# # # # Example:
# # # #     B24010_003E → mgmt_bus_fin_occ_est_B24010_003E

# # # # The compressed_label is built by:
# # # # 1. Stripping the Estimate/Margin of Error prefix (encoded as _est/_moe)
# # # # 2. Stripping a leading "Total:" segment (redundant in most tables)
# # # # 3. Tokenizing the remaining label hierarchy on !! separators
# # # # 4. Abbreviating tokens via the dictionary in abbreviations.py
# # # # 5. Truncating if the full name exceeds 63 characters (PostGIS limit)

# # # # The variable code suffix guarantees uniqueness.

# # # # Usage:
# # # #     from loci.collectors.census.client import CensusClient
# # # #     from loci.collectors.census.variable_mapper import CensusVariableMapper

# # # #     client = CensusClient(api_key="YOUR_KEY")
# # # #     mapper = CensusVariableMapper(client)

# # # #     mapping = mapper.build_mapping(spec)
# # # #     # {'B24010_003E': 'mgmt_bus_fin_occ_est_B24010_003E', ...}

# # # #     mapper.generate_staging_model(spec, mapping, output_dir="dbt/models/staging/census")
# # # # """

# # # # from __future__ import annotations

# # # # import logging
# # # # import os
# # # # import re

# # # # import requests

# # # # from loci.collectors.census.abbreviations import ABBREVIATIONS
# # # # from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# # # # logger = logging.getLogger(__name__)

# # # # POSTGRES_MAX_IDENTIFIER = 63


# # # # class CensusVariableMapper:
# # # #     """
# # # #     Maps Census variable codes to human-readable, PostGIS-safe column names.

# # # #     Parameters
# # # #     ----------
# # # #     client : CensusClient
# # # #         Used to resolve the full variable list for a spec.
# # # #     """

# # # #     BASE = "https://api.census.gov/data"

# # # #     def __init__(self, client):
# # # #         self.client = client
# # # #         self._session = requests.Session()
# # # #         self._label_cache: dict[str, dict[str, str]] = {}
# # # #         # Build case-insensitive lookup from the abbreviation dictionary
# # # #         self._abbrevs = {k.lower(): v for k, v in ABBREVIATIONS.items()}

# # # #     # ------------------------------------------------------------------ #
# # # #     #  Public API
# # # #     # ------------------------------------------------------------------ #

# # # #     def build_mapping(
# # # #         self,
# # # #         spec: CensusDatasetSpec,
# # # #         vintage: int | None = None,
# # # #     ) -> dict[str, str]:
# # # #         """
# # # #         Build a variable_code → column_name mapping for a spec.

# # # #         Parameters
# # # #         ----------
# # # #         spec : CensusDatasetSpec
# # # #         vintage : int, optional
# # # #             Which vintage to resolve variables and fetch labels for.
# # # #             Defaults to the most recent vintage in the spec.

# # # #         Returns
# # # #         -------
# # # #         dict[str, str]
# # # #             Mapping from original variable code to compressed column name.
# # # #         """
# # # #         vintage = vintage or max(spec.vintages)
# # # #         variables = self.client.resolve_all_variables(spec, vintage)
# # # #         labels = self._fetch_labels(spec.dataset, vintage, spec.groups, variables)

# # # #         mapping = {}
# # # #         for var in variables:
# # # #             label = labels.get(var, "")
# # # #             mapping[var] = self._compress(var, label)

# # # #         return mapping

# # # #     def generate_staging_model(
# # # #         self,
# # # #         spec: CensusDatasetSpec,
# # # #         mapping: dict[str, str],
# # # #         output_dir: str,
# # # #     ) -> str:
# # # #         """
# # # #         Write a dbt staging .sql file that aliases raw Census columns.

# # # #         Parameters
# # # #         ----------
# # # #         spec : CensusDatasetSpec
# # # #         mapping : dict[str, str]
# # # #             From build_mapping().
# # # #         output_dir : str
# # # #             Directory to write the .sql file into.

# # # #         Returns
# # # #         -------
# # # #         str
# # # #             Path to the written file.
# # # #         """
# # # #         geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
# # # #         geo_cols_sanitized = [
# # # #             self.client._sanitize_column_name(c) for c in geo_columns
# # # #         ]

# # # #         source_ref = (
# # # #             f"{{{{ source('{spec.target_schema}', '{spec.target_table}') }}}}"
# # # #         )

# # # #         # Build the select list
# # # #         select_lines = []

# # # #         # Geography columns pass through unchanged
# # # #         for col in geo_cols_sanitized:
# # # #             select_lines.append(f'        "{col}"')

# # # #         # Vintage and NAME pass through
# # # #         select_lines.append('        "vintage"')
# # # #         select_lines.append('        "NAME"')

# # # #         # Data columns get aliased
# # # #         for var_code, col_name in sorted(mapping.items()):
# # # #             select_lines.append(f'        "{var_code}" as "{col_name}"')

# # # #         select_body = ",\n".join(select_lines)

# # # #         sql = (
# # # #             f"with aliased as (\n"
# # # #             f"    select\n"
# # # #             f"{select_body}\n"
# # # #             f"    from {source_ref}\n"
# # # #             f")\n"
# # # #             f"\n"
# # # #             f"select * from aliased\n"
# # # #         )

# # # #         os.makedirs(output_dir, exist_ok=True)
# # # #         filename = f"{spec.name}.sql"
# # # #         filepath = os.path.join(output_dir, filename)

# # # #         with open(filepath, "w") as f:
# # # #             f.write(sql)

# # # #         logger.info("Wrote staging model to %s (%d columns)", filepath, len(mapping))
# # # #         return filepath

# # # #     # ------------------------------------------------------------------ #
# # # #     #  Label fetching
# # # #     # ------------------------------------------------------------------ #

# # # #     def _fetch_labels(
# # # #         self,
# # # #         dataset: str,
# # # #         vintage: int,
# # # #         groups: list[str],
# # # #         variables: list[str],
# # # #     ) -> dict[str, str]:
# # # #         """
# # # #         Fetch labels for all variables, keyed by variable code.

# # # #         Fetches each group's metadata from the Census API.
# # # #         """
# # # #         labels: dict[str, str] = {}

# # # #         for group in groups:
# # # #             cache_key = f"{dataset}/{vintage}/{group}"
# # # #             if cache_key not in self._label_cache:
# # # #                 self._label_cache[cache_key] = self._fetch_group_labels(
# # # #                     dataset, vintage, group
# # # #                 )
# # # #             labels.update(self._label_cache[cache_key])

# # # #         # For any variables not covered by the groups (i.e. individually
# # # #         # specified variables), we'd need to fetch from the full variables
# # # #         # endpoint. For now, they'll get an empty label and just use the
# # # #         # variable code with est/moe suffix.
# # # #         return labels

# # # #     def _fetch_group_labels(
# # # #         self,
# # # #         dataset: str,
# # # #         vintage: int,
# # # #         group: str,
# # # #     ) -> dict[str, str]:
# # # #         """Fetch variable labels for a single group."""
# # # #         url = f"{self.BASE}/{vintage}/{dataset}/groups/{group}.json"
# # # #         resp = self._session.get(url)
# # # #         resp.raise_for_status()
# # # #         data = resp.json()
# # # #         variables = data.get("variables", {})
# # # #         return {
# # # #             name: info.get("label", "")
# # # #             for name, info in variables.items()
# # # #             if name != group
# # # #         }

# # # #     # ------------------------------------------------------------------ #
# # # #     #  Label compression
# # # #     # ------------------------------------------------------------------ #

# # # #     def _compress(self, variable: str, label: str) -> str:
# # # #         """
# # # #         Compress a Census label + variable code into a PostGIS-safe column name.

# # # #         Format: {compressed_label}_{est|moe}_{variable_code}
# # # #         """
# # # #         suffix_type = self._extract_suffix_type(variable)
# # # #         label_part = self._compress_label(label)
# # # #         code_part = variable

# # # #         if label_part:
# # # #             full_name = f"{label_part}_{suffix_type}_{code_part}"
# # # #         else:
# # # #             full_name = f"{suffix_type}_{code_part}"

# # # #         # Truncate label portion if we exceed the limit
# # # #         if len(full_name) > POSTGRES_MAX_IDENTIFIER:
# # # #             # The suffix we must keep: _{suffix_type}_{code_part}
# # # #             reserved = f"_{suffix_type}_{code_part}"
# # # #             max_label_len = POSTGRES_MAX_IDENTIFIER - len(reserved)
# # # #             if max_label_len > 0:
# # # #                 label_part = label_part[:max_label_len].rstrip("_")
# # # #                 full_name = f"{label_part}{reserved}"
# # # #             else:
# # # #                 full_name = f"{suffix_type}_{code_part}"

# # # #         return full_name.lower()

# # # #     def _compress_label(self, label: str) -> str:
# # # #         """
# # # #         Strip prefixes, tokenize, abbreviate, and join a Census label.
# # # #         """
# # # #         if not label:
# # # #             return ""

# # # #         # Strip Estimate/Margin of Error prefix
# # # #         label = re.sub(r"^Estimate!!", "", label)
# # # #         label = re.sub(r"^Margin of Error!!", "", label)

# # # #         # Strip leading "Total:" or "Total" segment
# # # #         label = re.sub(r"^Total:?(!|$)", "", label).lstrip("!")

# # # #         # Split on !! hierarchy separators
# # # #         segments = [s.strip() for s in label.split("!!") if s.strip()]

# # # #         tokens = []
# # # #         for segment in segments:
# # # #             # Strip trailing colons
# # # #             segment = segment.rstrip(":")
# # # #             # Split on spaces and punctuation (keeping numbers)
# # # #             words = re.split(r"[\s,;()\-/]+", segment)
# # # #             for word in words:
# # # #                 if not word:
# # # #                     continue
# # # #                 abbreviated = self._abbreviate(word)
# # # #                 if abbreviated:  # skip tokens that abbreviate to ""
# # # #                     tokens.append(abbreviated)

# # # #         return "_".join(tokens)

# # # #     def _abbreviate(self, word: str) -> str:
# # # #         """Look up a word in the abbreviation dictionary (case-insensitive)."""
# # # #         # Strip possessives and trailing punctuation
# # # #         cleaned = re.sub(r"['']s$", "", word).strip(".:,;")
# # # #         lookup = cleaned.lower()

# # # #         if lookup in self._abbrevs:
# # # #             return self._abbrevs[lookup]

# # # #         # If it's a pure number, keep as-is
# # # #         if cleaned.isdigit():
# # # #             return cleaned

# # # #         # Not in dictionary — lowercase and return as-is
# # # #         return cleaned.lower()

# # # #     @staticmethod
# # # #     def _extract_suffix_type(variable: str) -> str:
# # # #         """Extract est/moe from the variable code suffix."""
# # # #         if variable.endswith("M") and not variable.endswith("PM"):
# # # #             return "moe"
# # # #         return "est"


# # # ############################################################################


# # # """
# # # CensusVariableMapper — maps Census variable codes to human-readable column names,
# # # and generates dbt staging models that apply those mappings.

# # # Two-pass column name strategy:

# # #     Pass 1 — Group prefix: compress the group's "concept" field into a short
# # #     prefix (e.g. "SEX BY OCCUPATION FOR THE CIVILIAN EMPLOYED POPULATION 16
# # #     YEARS AND OVER" → "sex_by_occ_civ_emp").

# # #     Pass 2 — Leaf label: for each variable within a group, identify the
# # #     minimum label segments needed to differentiate it from siblings in the
# # #     label tree. Drop segments where the tree doesn't branch (shared context).

# # # Final column name format:
# # #     {group_prefix}__{leaf_label}_{est|moe}_{variable_code}

# # # Example:
# # #     B24012_005E → sex_occ_civ_emp__m_mgmt_occ_est_b24012_005e

# # # The variable code suffix guarantees uniqueness. If the full name exceeds
# # # PostgreSQL's 63-character limit, the leaf label is truncated.

# # # Usage:
# # #     from loci.collectors.census.client import CensusClient
# # #     from loci.collectors.census.variable_mapper import CensusVariableMapper

# # #     client = CensusClient(api_key="YOUR_KEY")
# # #     mapper = CensusVariableMapper(client)

# # #     mapping = mapper.build_mapping(spec)
# # #     mapper.generate_staging_model(spec, mapping, output_dir="dbt/models/staging/census")
# # # """

# # # from __future__ import annotations

# # # import logging
# # # import os
# # # import re
# # # from collections import defaultdict

# # # import requests

# # # from loci.collectors.census.abbreviations import ABBREVIATIONS
# # # from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# # # logger = logging.getLogger(__name__)

# # # POSTGRES_MAX_IDENTIFIER = 63
# # # GROUP_PREFIX_MAX_LEN = 15


# # # class CensusVariableMapper:
# # #     """
# # #     Maps Census variable codes to human-readable, PostGIS-safe column names.

# # #     Parameters
# # #     ----------
# # #     client : CensusClient
# # #         Used to resolve the full variable list for a spec.
# # #     """

# # #     BASE = "https://api.census.gov/data"

# # #     def __init__(self, client):
# # #         self.client = client
# # #         self._session = requests.Session()
# # #         self._group_metadata_cache: dict[str, dict] = {}
# # #         # Build case-insensitive lookup from the abbreviation dictionary
# # #         self._abbrevs = {k.lower(): v for k, v in ABBREVIATIONS.items()}

# # #     # ------------------------------------------------------------------ #
# # #     #  Public API
# # #     # ------------------------------------------------------------------ #

# # #     def build_mapping(
# # #         self,
# # #         spec: CensusDatasetSpec,
# # #         vintage: int | None = None,
# # #     ) -> dict[str, str]:
# # #         """
# # #         Build a variable_code → column_name mapping for a spec.

# # #         Parameters
# # #         ----------
# # #         spec : CensusDatasetSpec
# # #         vintage : int, optional
# # #             Which vintage to resolve variables and fetch labels for.
# # #             Defaults to the most recent vintage in the spec.

# # #         Returns
# # #         -------
# # #         dict[str, str]
# # #             Mapping from original variable code to compressed column name.
# # #         """
# # #         vintage = vintage or max(spec.vintages)
# # #         variables = self.client.resolve_all_variables(spec, vintage)

# # #         # Fetch metadata per group
# # #         group_meta: dict[str, dict] = {}
# # #         for group in spec.groups:
# # #             group_meta[group] = self._fetch_group_metadata(
# # #                 spec.dataset, vintage, group
# # #             )

# # #         # Assign each variable to its group
# # #         vars_by_group: dict[str, list[str]] = defaultdict(list)
# # #         group_for_var: dict[str, str] = {}
# # #         for var in variables:
# # #             assigned = False
# # #             for group, meta in group_meta.items():
# # #                 if var in meta["variables"]:
# # #                     vars_by_group[group].append(var)
# # #                     group_for_var[var] = group
# # #                     assigned = True
# # #                     break
# # #             if not assigned:
# # #                 # Individual variable not in any group
# # #                 vars_by_group["_ungrouped"].append(var)

# # #         # Pass 1: build group prefixes from concept fields
# # #         group_prefixes: dict[str, str] = {}
# # #         for group, meta in group_meta.items():
# # #             concept = meta.get("concept", group)
# # #             group_prefixes[group] = self._compress_concept(concept)
# # #         group_prefixes["_ungrouped"] = "misc"

# # #         # Pass 2: for each group, build the label tree and find distinguishing
# # #         # segments per variable
# # #         mapping: dict[str, str] = {}
# # #         for group, group_vars in vars_by_group.items():
# # #             meta = group_meta.get(group, {})
# # #             labels = meta.get("variables", {})

# # #             # Parse labels into segment lists (stripping Estimate/MOE/Total prefix)
# # #             parsed_segments = self._parse_group_labels(group_vars, labels)

# # #             # Find minimum distinguishing segments per variable
# # #             distinguishing = self._find_distinguishing_segments(parsed_segments)

# # #             # Build the final column name for each variable
# # #             prefix = group_prefixes[group]
# # #             for var in group_vars:
# # #                 leaf_segments = distinguishing.get(var, [])
# # #                 mapping[var] = self._build_column_name(
# # #                     var, prefix, leaf_segments
# # #                 )

# # #         return mapping

# # #     def generate_staging_model(
# # #         self,
# # #         spec: CensusDatasetSpec,
# # #         mapping: dict[str, str],
# # #         output_dir: str,
# # #     ) -> str:
# # #         """
# # #         Write a dbt staging .sql file that aliases raw Census columns.

# # #         Parameters
# # #         ----------
# # #         spec : CensusDatasetSpec
# # #         mapping : dict[str, str]
# # #             From build_mapping().
# # #         output_dir : str
# # #             Directory to write the .sql file into.

# # #         Returns
# # #         -------
# # #         str
# # #             Path to the written file.
# # #         """
# # #         geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
# # #         geo_cols_sanitized = [
# # #             self.client._sanitize_column_name(c) for c in geo_columns
# # #         ]

# # #         source_ref = (
# # #             f"{{{{ source('{spec.target_schema}', '{spec.target_table}') }}}}"
# # #         )

# # #         # Build the select list
# # #         select_lines = []

# # #         # Geography columns pass through unchanged
# # #         for col in geo_cols_sanitized:
# # #             select_lines.append(f'        "{col}"')

# # #         # Vintage and NAME pass through
# # #         select_lines.append('        "vintage"')
# # #         select_lines.append('        "NAME"')

# # #         # Data columns get aliased
# # #         for var_code, col_name in sorted(mapping.items()):
# # #             select_lines.append(f'        "{var_code}" as "{col_name}"')

# # #         select_body = ",\n".join(select_lines)

# # #         sql = (
# # #             f"with aliased as (\n"
# # #             f"    select\n"
# # #             f"{select_body}\n"
# # #             f"    from {source_ref}\n"
# # #             f")\n"
# # #             f"\n"
# # #             f"select * from aliased\n"
# # #         )

# # #         os.makedirs(output_dir, exist_ok=True)
# # #         filename = f"{spec.name}.sql"
# # #         filepath = os.path.join(output_dir, filename)

# # #         with open(filepath, "w") as f:
# # #             f.write(sql)

# # #         logger.info("Wrote staging model to %s (%d columns)", filepath, len(mapping))
# # #         return filepath

# # #     # ------------------------------------------------------------------ #
# # #     #  Metadata fetching
# # #     # ------------------------------------------------------------------ #

# # #     def _fetch_group_metadata(
# # #         self,
# # #         dataset: str,
# # #         vintage: int,
# # #         group: str,
# # #     ) -> dict:
# # #         """
# # #         Fetch and cache a group's metadata: concept, variable labels.

# # #         Returns dict with keys: "concept", "variables" (dict of var_code → label).
# # #         """
# # #         cache_key = f"{dataset}/{vintage}/{group}"
# # #         if cache_key in self._group_metadata_cache:
# # #             return self._group_metadata_cache[cache_key]

# # #         url = f"{self.BASE}/{vintage}/{dataset}/groups/{group}.json"
# # #         resp = self._session.get(url)
# # #         resp.raise_for_status()
# # #         data = resp.json()

# # #         variables_raw = data.get("variables", {})
# # #         concept = data.get("description", "") or data.get("concept", "") or group

# # #         variables = {
# # #             name: info.get("label", "")
# # #             for name, info in variables_raw.items()
# # #             if name != group and name != "NAME"
# # #         }

# # #         result = {"concept": concept, "variables": variables}
# # #         self._group_metadata_cache[cache_key] = result
# # #         return result

# # #     # ------------------------------------------------------------------ #
# # #     #  Pass 1: Group concept → prefix
# # #     # ------------------------------------------------------------------ #

# # #     def _compress_concept(self, concept: str) -> str:
# # #         """
# # #         Compress a group concept string into a short prefix.

# # #         Example: "SEX BY OCCUPATION FOR THE CIVILIAN EMPLOYED POPULATION
# # #         16 YEARS AND OVER" → "sex_by_occ_civ_emp"
# # #         """
# # #         abbreviated = self._abbreviate_segment(concept)
# # #         if len(abbreviated) > GROUP_PREFIX_MAX_LEN:
# # #             abbreviated = abbreviated[:GROUP_PREFIX_MAX_LEN].rstrip("_")
# # #         return abbreviated

# # #     # ------------------------------------------------------------------ #
# # #     #  Pass 2: Label tree → distinguishing segments
# # #     # ------------------------------------------------------------------ #

# # #     def _parse_group_labels(
# # #         self,
# # #         variables: list[str],
# # #         labels: dict[str, str],
# # #     ) -> dict[str, list[str]]:
# # #         """
# # #         Parse raw Census labels into cleaned segment lists.

# # #         Strips the Estimate/MOE prefix and leading Total: segment.
# # #         Only processes the E (estimate) version of each variable to build
# # #         the tree — the M (MOE) version mirrors it exactly.
# # #         """
# # #         parsed: dict[str, list[str]] = {}
# # #         for var in variables:
# # #             label = labels.get(var, "")

# # #             # Strip Estimate!! / Margin of Error!! prefix
# # #             label = re.sub(r"^Estimate!!", "", label)
# # #             label = re.sub(r"^Margin of Error!!", "", label)

# # #             # Strip leading Total: segment
# # #             label = re.sub(r"^Total:?(!|$)", "", label).lstrip("!")

# # #             # Split on !! and clean each segment
# # #             segments = []
# # #             for seg in label.split("!!"):
# # #                 seg = seg.strip().rstrip(":")
# # #                 if seg:
# # #                     segments.append(seg)

# # #             parsed[var] = segments

# # #         return parsed

# # #     def _find_distinguishing_segments(
# # #         self,
# # #         parsed: dict[str, list[str]],
# # #     ) -> dict[str, list[str]]:
# # #         """
# # #         For each variable, find the minimum set of segments needed to
# # #         differentiate it from all other variables in the group.

# # #         Strategy: walk from root to leaf. Keep a segment only if the tree
# # #         branches at that level (i.e. there are multiple distinct values
# # #         among variables sharing the same prefix up to that depth). Always
# # #         keep the leaf segment.
# # #         """
# # #         result: dict[str, list[str]] = {}

# # #         for var, segs in parsed.items():
# # #             if not segs:
# # #                 result[var] = ["total"]
# # #                 continue

# # #             kept = []
# # #             for depth in range(len(segs)):
# # #                 prefix = tuple(segs[:depth])

# # #                 # Count distinct values at this depth for this prefix
# # #                 siblings = set()
# # #                 for other_segs in parsed.values():
# # #                     if (
# # #                         len(other_segs) > depth
# # #                         and tuple(other_segs[:depth]) == prefix
# # #                     ):
# # #                         siblings.add(other_segs[depth])

# # #                 if len(siblings) > 1:
# # #                     kept.append(segs[depth])

# # #             # Always include the leaf segment if not already kept
# # #             if segs and (not kept or kept[-1] != segs[-1]):
# # #                 kept.append(segs[-1])

# # #             result[var] = kept

# # #         return result

# # #     # ------------------------------------------------------------------ #
# # #     #  Column name assembly
# # #     # ------------------------------------------------------------------ #

# # #     def _build_column_name(
# # #         self,
# # #         variable: str,
# # #         group_prefix: str,
# # #         leaf_segments: list[str],
# # #     ) -> str:
# # #         """
# # #         Assemble the final column name from group prefix, leaf, suffix type,
# # #         and variable code. Truncates the leaf if the result exceeds 63 chars.

# # #         Format: {group_prefix}__{leaf}_{est|moe}_{variable_code}
# # #         """
# # #         suffix_type = self._extract_suffix_type(variable)
# # #         leaf = "_".join(self._abbreviate_segment(s) for s in leaf_segments)
# # #         code = variable

# # #         if leaf:
# # #             full_name = f"{group_prefix}__{leaf}_{suffix_type}_{code}"
# # #         else:
# # #             full_name = f"{group_prefix}__{suffix_type}_{code}"

# # #         # Truncate leaf portion if we exceed the limit
# # #         if len(full_name) > POSTGRES_MAX_IDENTIFIER:
# # #             # The parts we must keep
# # #             reserved = f"__{suffix_type}_{code}"
# # #             prefix_reserved = f"{group_prefix}{reserved}"
# # #             max_leaf_len = POSTGRES_MAX_IDENTIFIER - len(prefix_reserved)
# # #             if max_leaf_len > 1:
# # #                 # Leave room for the _ between prefix and leaf
# # #                 leaf_truncated = leaf[: max_leaf_len - 1].rstrip("_")
# # #                 full_name = (
# # #                     f"{group_prefix}__{leaf_truncated}_{suffix_type}_{code}"
# # #                 )
# # #             else:
# # #                 full_name = f"{group_prefix}{reserved}"

# # #         return full_name.lower()

# # #     # ------------------------------------------------------------------ #
# # #     #  Text compression helpers
# # #     # ------------------------------------------------------------------ #

# # #     def _abbreviate_segment(self, segment: str) -> str:
# # #         """Abbreviate all words in a segment and join with underscores."""
# # #         words = re.split(r"[\s,;()\-/]+", segment)
# # #         tokens = []
# # #         for word in words:
# # #             if not word:
# # #                 continue
# # #             abbreviated = self._abbreviate_word(word)
# # #             if abbreviated:  # skip tokens that abbreviate to ""
# # #                 tokens.append(abbreviated)
# # #         return "_".join(tokens)

# # #     def _abbreviate_word(self, word: str) -> str:
# # #         """Look up a single word in the abbreviation dictionary."""
# # #         # Strip possessives and trailing punctuation
# # #         cleaned = re.sub(r"['\u2019]s$", "", word).strip(".:,;")
# # #         lookup = cleaned.lower()

# # #         if lookup in self._abbrevs:
# # #             return self._abbrevs[lookup]

# # #         # Pure numbers pass through
# # #         if cleaned.isdigit():
# # #             return cleaned

# # #         # Not in dictionary — lowercase as-is
# # #         return cleaned.lower()

# # #     @staticmethod
# # #     def _extract_suffix_type(variable: str) -> str:
# # #         """Extract est/moe from the variable code suffix."""
# # #         if variable.endswith("M") and not variable.endswith("PM"):
# # #             return "moe"
# # #         return "est"


# # """
# # CensusVariableMapper — maps Census variable codes to human-readable column names,
# # and generates dbt staging models that apply those mappings.

# # Two-pass column name strategy:

# #     Pass 1 — Group prefix: compress the group's "concept" field into a short
# #     prefix (e.g. "SEX BY OCCUPATION FOR THE CIVILIAN EMPLOYED POPULATION 16
# #     YEARS AND OVER" → "sex_occ_civ_emp").

# #     Pass 2 — Distinguishing leaf: for each variable within a group, parse
# #     the label hierarchy into a tree. At each branching level, find the minimum
# #     abbreviated tokens needed to distinguish this variable's segment from its
# #     siblings. Deduplicate consecutive repeated tokens.

# # Final column name format:
# #     {group_prefix}__{distinguishing_tokens}_{est|moe}_{variable_code}

# # Example:
# #     B24012_005E → sex_occ_civ_emp__m_mgmt_est_b24012_005e

# # The variable code suffix guarantees uniqueness. If the full name exceeds
# # PostgreSQL's 63-character limit, the leaf portion is truncated.

# # Usage:
# #     from loci.collectors.census.client import CensusClient
# #     from loci.collectors.census.variable_mapper import CensusVariableMapper

# #     client = CensusClient(api_key="YOUR_KEY")
# #     mapper = CensusVariableMapper(client)

# #     mapping = mapper.build_mapping(spec)
# #     mapper.generate_staging_model(spec, mapping, output_dir="dbt/models/staging/census")
# # """

# # from __future__ import annotations

# # import logging
# # import os
# # import re
# # from collections import defaultdict

# # import requests

# # from loci.collectors.census.abbreviations import ABBREVIATIONS
# # from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# # logger = logging.getLogger(__name__)

# # POSTGRES_MAX_IDENTIFIER = 63
# # GROUP_PREFIX_MAX_LEN = 16

# # # Small filler words that don't help distinguish segments from siblings.
# # FILLER_TOKENS = frozenset({"and", "or", "for", "by", "in", "to", "the", "of", ""})


# # class CensusVariableMapper:
# #     """
# #     Maps Census variable codes to human-readable, PostGIS-safe column names.

# #     Parameters
# #     ----------
# #     client : CensusClient
# #         Used to resolve the full variable list for a spec.
# #     """

# #     BASE = "https://api.census.gov/data"

# #     def __init__(self, client):
# #         self.client = client
# #         self._session = requests.Session()
# #         self._group_metadata_cache: dict[str, dict] = {}
# #         self._abbrevs = {k.lower(): v for k, v in ABBREVIATIONS.items()}

# #     # ------------------------------------------------------------------ #
# #     #  Public API
# #     # ------------------------------------------------------------------ #

# #     def build_mapping(
# #         self,
# #         spec: CensusDatasetSpec,
# #         vintage: int | None = None,
# #     ) -> dict[str, str]:
# #         """
# #         Build a variable_code → column_name mapping for a spec.

# #         Parameters
# #         ----------
# #         spec : CensusDatasetSpec
# #         vintage : int, optional
# #             Defaults to the most recent vintage in the spec.

# #         Returns
# #         -------
# #         dict[str, str]
# #         """
# #         vintage = vintage or max(spec.vintages)
# #         variables = self.client.resolve_all_variables(spec, vintage)

# #         # Fetch metadata per group
# #         group_meta: dict[str, dict] = {}
# #         for group in spec.groups:
# #             group_meta[group] = self._fetch_group_metadata(
# #                 spec.dataset, vintage, group
# #             )

# #         # Assign each variable to its group
# #         vars_by_group: dict[str, list[str]] = defaultdict(list)
# #         for var in variables:
# #             assigned = False
# #             for group, meta in group_meta.items():
# #                 if var in meta["labels"]:
# #                     vars_by_group[group].append(var)
# #                     assigned = True
# #                     break
# #             if not assigned:
# #                 vars_by_group["_ungrouped"].append(var)

# #         # Pass 1: group prefixes from concept
# #         group_prefixes: dict[str, str] = {}
# #         for group, meta in group_meta.items():
# #             group_prefixes[group] = self._compress_concept(meta["concept"])
# #         group_prefixes["_ungrouped"] = "misc"

# #         # Pass 2: build label tree per group, find distinguishing tokens
# #         mapping: dict[str, str] = {}
# #         for group, group_vars in vars_by_group.items():
# #             labels = group_meta.get(group, {}).get("labels", {})
# #             parsed = self._parse_labels(group_vars, labels)
# #             distinguishing = self._find_distinguishing_tokens(parsed)

# #             prefix = group_prefixes[group]
# #             for var in group_vars:
# #                 tokens = distinguishing.get(var, [])
# #                 mapping[var] = self._build_column_name(var, prefix, tokens)

# #         return mapping

# #     def generate_staging_model(
# #         self,
# #         spec: CensusDatasetSpec,
# #         mapping: dict[str, str],
# #         output_dir: str,
# #     ) -> str:
# #         """
# #         Write a dbt staging .sql file that aliases raw Census columns.

# #         Returns the path to the written file.
# #         """
# #         geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
# #         geo_cols_sanitized = [
# #             self.client._sanitize_column_name(c) for c in geo_columns
# #         ]

# #         source_ref = (
# #             f"{{{{ source('{spec.target_schema}', '{spec.target_table}') }}}}"
# #         )

# #         select_lines = []
# #         for col in geo_cols_sanitized:
# #             select_lines.append(f'        "{col}"')
# #         select_lines.append('        "vintage"')
# #         select_lines.append('        "NAME"')
# #         for var_code, col_name in sorted(mapping.items()):
# #             select_lines.append(f'        "{var_code}" as "{col_name}"')

# #         select_body = ",\n".join(select_lines)

# #         sql = (
# #             f"with aliased as (\n"
# #             f"    select\n"
# #             f"{select_body}\n"
# #             f"    from {source_ref}\n"
# #             f")\n"
# #             f"\n"
# #             f"select * from aliased\n"
# #         )

# #         os.makedirs(output_dir, exist_ok=True)
# #         filepath = os.path.join(output_dir, f"{spec.name}.sql")
# #         with open(filepath, "w") as f:
# #             f.write(sql)

# #         logger.info("Wrote staging model to %s (%d columns)", filepath, len(mapping))
# #         return filepath

# #     # ------------------------------------------------------------------ #
# #     #  Metadata fetching
# #     # ------------------------------------------------------------------ #

# #     def _fetch_group_metadata(
# #         self,
# #         dataset: str,
# #         vintage: int,
# #         group: str,
# #     ) -> dict:
# #         """
# #         Fetch and cache a group's metadata.

# #         Returns dict with "concept" (str) and "labels" (dict[var_code, label]).
# #         """
# #         cache_key = f"{dataset}/{vintage}/{group}"
# #         if cache_key in self._group_metadata_cache:
# #             return self._group_metadata_cache[cache_key]

# #         url = f"{self.BASE}/{vintage}/{dataset}/groups/{group}.json"
# #         resp = self._session.get(url)
# #         resp.raise_for_status()
# #         data = resp.json()

# #         variables_raw = data.get("variables", {})

# #         # The concept lives on each variable entry, not at the top level.
# #         # Extract it from the first variable that has one.
# #         concept = group  # fallback
# #         labels: dict[str, str] = {}
# #         for name, info in variables_raw.items():
# #             if name == group or name == "NAME":
# #                 continue
# #             labels[name] = info.get("label", "")
# #             if concept == group:
# #                 var_concept = info.get("concept", "")
# #                 if var_concept:
# #                     concept = var_concept

# #         result = {"concept": concept, "labels": labels}
# #         self._group_metadata_cache[cache_key] = result
# #         return result

# #     # ------------------------------------------------------------------ #
# #     #  Pass 1: concept → group prefix
# #     # ------------------------------------------------------------------ #

# #     def _compress_concept(self, concept: str) -> str:
# #         """Compress a group concept into a short prefix (max GROUP_PREFIX_MAX_LEN chars)."""
# #         tokens = self._get_content_tokens(concept)
# #         result = "_".join(tokens)
# #         if len(result) > GROUP_PREFIX_MAX_LEN:
# #             result = result[:GROUP_PREFIX_MAX_LEN].rstrip("_")
# #         return result

# #     # ------------------------------------------------------------------ #
# #     #  Pass 2: label tree → distinguishing tokens
# #     # ------------------------------------------------------------------ #

# #     def _parse_labels(
# #         self,
# #         variables: list[str],
# #         labels: dict[str, str],
# #     ) -> dict[str, list[str]]:
# #         """
# #         Parse Census labels into cleaned segment lists, stripping
# #         Estimate/MOE prefix and leading Total: segment.
# #         """
# #         parsed: dict[str, list[str]] = {}
# #         for var in variables:
# #             label = labels.get(var, "")
# #             label = re.sub(r"^Estimate!!", "", label)
# #             label = re.sub(r"^Margin of Error!!", "", label)
# #             label = re.sub(r"^Total:?(!|$)", "", label).lstrip("!")

# #             segments = []
# #             for seg in label.split("!!"):
# #                 seg = seg.strip().rstrip(":")
# #                 if seg:
# #                     segments.append(seg)

# #             parsed[var] = segments
# #         return parsed

# #     def _find_distinguishing_tokens(
# #         self,
# #         parsed: dict[str, list[str]],
# #     ) -> dict[str, list[str]]:
# #         """
# #         For each variable, walk the label tree. At each branching level,
# #         find the minimum abbreviated tokens needed to distinguish this
# #         variable's segment from its siblings. Deduplicate consecutive
# #         repeated tokens at the end.
# #         """
# #         result: dict[str, list[str]] = {}

# #         for var, segs in parsed.items():
# #             if not segs:
# #                 result[var] = ["tot"]
# #                 continue

# #             all_tokens: list[str] = []
# #             for depth in range(len(segs)):
# #                 prefix = tuple(segs[:depth])

# #                 # Collect sibling segments at this depth (same prefix, different values)
# #                 siblings: list[str] = []
# #                 for other_var, other_segs in parsed.items():
# #                     if other_var == var:
# #                         continue
# #                     if (
# #                         len(other_segs) > depth
# #                         and tuple(other_segs[:depth]) == prefix
# #                         and other_segs[depth] != segs[depth]
# #                     ):
# #                         if other_segs[depth] not in siblings:
# #                             siblings.append(other_segs[depth])

# #                 if not siblings:
# #                     # Non-branching level — skip (unless it's the leaf)
# #                     is_leaf = depth == len(segs) - 1
# #                     if is_leaf:
# #                         # Leaf of a non-branching path: use full abbreviated tokens
# #                         all_tokens.extend(self._get_content_tokens(segs[depth]))
# #                     continue

# #                 # Branching level: find minimum tokens to distinguish from siblings
# #                 distinguishing = self._min_distinguishing_tokens(
# #                     segs[depth], siblings
# #                 )
# #                 all_tokens.extend(distinguishing)

# #             # Always ensure the leaf is represented
# #             if segs and not all_tokens:
# #                 all_tokens = self._get_content_tokens(segs[-1])

# #             # Deduplicate consecutive identical tokens
# #             all_tokens = self._dedup_consecutive(all_tokens)

# #             result[var] = all_tokens

# #         return result

# #     def _min_distinguishing_tokens(
# #         self,
# #         segment: str,
# #         sibling_segments: list[str],
# #     ) -> list[str]:
# #         """
# #         Find the minimum number of content tokens from the abbreviated segment
# #         that distinguish it from all siblings.
# #         """
# #         my_tokens = self._get_content_tokens(segment)
# #         if not my_tokens:
# #             return []

# #         sibling_token_lists = [
# #             self._get_content_tokens(s) for s in sibling_segments
# #         ]

# #         for n in range(1, len(my_tokens) + 1):
# #             candidate = my_tokens[:n]
# #             is_unique = all(
# #                 sib[:n] != candidate for sib in sibling_token_lists
# #             )
# #             if is_unique:
# #                 return candidate

# #         return my_tokens

# #     # ------------------------------------------------------------------ #
# #     #  Column name assembly
# #     # ------------------------------------------------------------------ #

# #     def _build_column_name(
# #         self,
# #         variable: str,
# #         group_prefix: str,
# #         tokens: list[str],
# #     ) -> str:
# #         """
# #         Assemble: {group_prefix}__{tokens}_{est|moe}_{variable_code}

# #         Truncates the token portion if the result exceeds 63 chars.
# #         """
# #         suffix_type = self._extract_suffix_type(variable)
# #         leaf = "_".join(tokens) if tokens else "tot"
# #         code = variable

# #         full_name = f"{group_prefix}__{leaf}_{suffix_type}_{code}"

# #         if len(full_name) > POSTGRES_MAX_IDENTIFIER:
# #             reserved = f"__{suffix_type}_{code}"
# #             max_leaf = POSTGRES_MAX_IDENTIFIER - len(group_prefix) - len(reserved) - 1
# #             if max_leaf > 0:
# #                 leaf_truncated = leaf[:max_leaf].rstrip("_")
# #                 full_name = f"{group_prefix}__{leaf_truncated}_{suffix_type}_{code}"
# #             else:
# #                 full_name = f"{group_prefix}{reserved}"

# #         return full_name.lower()

# #     # ------------------------------------------------------------------ #
# #     #  Text helpers
# #     # ------------------------------------------------------------------ #

# #     def _get_content_tokens(self, segment: str) -> list[str]:
# #         """
# #         Split a segment into words, abbreviate each, and return only
# #         content tokens (skipping filler words like 'and', 'of', 'the').
# #         """
# #         words = re.split(r"[\s,;()\-/]+", segment)
# #         tokens = []
# #         for word in words:
# #             if not word:
# #                 continue
# #             abbreviated = self._abbreviate_word(word)
# #             if abbreviated and abbreviated not in FILLER_TOKENS:
# #                 tokens.append(abbreviated)
# #         return tokens

# #     def _abbreviate_word(self, word: str) -> str:
# #         """Look up a single word in the abbreviation dictionary."""
# #         cleaned = re.sub(r"['\u2019]s$", "", word).strip(".:,;")
# #         lookup = cleaned.lower()
# #         if lookup in self._abbrevs:
# #             return self._abbrevs[lookup]
# #         if cleaned.isdigit():
# #             return cleaned
# #         return cleaned.lower()

# #     @staticmethod
# #     def _extract_suffix_type(variable: str) -> str:
# #         """Extract est/moe from the variable code suffix."""
# #         if variable.endswith("M") and not variable.endswith("PM"):
# #             return "moe"
# #         return "est"

# #     @staticmethod
# #     def _dedup_consecutive(tokens: list[str]) -> list[str]:
# #         """Remove consecutive duplicate tokens: [a, a, b] → [a, b]."""
# #         if not tokens:
# #             return tokens
# #         result = [tokens[0]]
# #         for t in tokens[1:]:
# #             if t != result[-1]:
# #                 result.append(t)
# #         return result


# """
# CensusVariableMapper — maps Census variable codes to human-readable column names,
# and generates dbt staging models that apply those mappings.

# Two-pass column name strategy:

#     Pass 1 — Group prefix: compress the group's "concept" field into a short
#     prefix (e.g. "SEX BY OCCUPATION ... 16 YEARS AND OVER" → "sex_occ_med_earn").

#     Pass 2 — Distinguishing leaf: parse the label hierarchy into a tree, then
#     for each variable:
#       - Distant branching ancestors: use the minimum tokens needed to distinguish
#         from siblings (typically 1 token).
#       - Immediate parent branching level: use up to PARENT_SCOPE_TOKENS (3)
#         to convey the scope of the category.
#       - Non-branching leaf: use up to PARENT_SCOPE_TOKENS for its content.
#       - Category totals: append "tot" (detected automatically when a variable
#         has children in the tree).
#       - Deduplicate consecutive repeated tokens (protecting the final token).

# Final column name format:
#     {group_prefix}__{distinguishing_tokens}_{est|moe}_{variable_code}

# Examples:
#     B24012_005E → sex_occ_med_earn__m_mgmt_occ_est_b24012_005e
#     B24012_034E → sex_occ_med_earn__m_prod_transp_matrl_tot_est_b24012_034e
#     B24012_073E → sex_occ_med_earn__f_prod_matrl_mov_occ_est_b24012_073e

# Usage:
#     from loci.collectors.census.client import CensusClient
#     from loci.collectors.census.variable_mapper import CensusVariableMapper

#     client = CensusClient(api_key="YOUR_KEY")
#     mapper = CensusVariableMapper(client)

#     mapping = mapper.build_mapping(spec)
#     mapper.generate_staging_model(spec, mapping, output_dir="dbt/models/staging/census")
# """

# from __future__ import annotations

# import logging
# import os
# import re
# from collections import defaultdict

# import requests

# from loci.collectors.census.abbreviations import ABBREVIATIONS
# from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# logger = logging.getLogger(__name__)

# POSTGRES_MAX_IDENTIFIER = 63
# GROUP_PREFIX_MAX_LEN = 16

# # Maximum content tokens kept for the immediate parent branching level
# # and for non-branching leaf segments. Distant ancestors use fewer
# # (minimum distinguishing, typically 1).
# PARENT_SCOPE_TOKENS = 3

# # Small filler words that don't help distinguish segments from siblings.
# FILLER_TOKENS = frozenset({"and", "or", "for", "by", "in", "to", "the", "of", "", "means", "mns", "sex"})


# class CensusVariableMapper:
#     """
#     Maps Census variable codes to human-readable, PostGIS-safe column names.

#     Parameters
#     ----------
#     client : CensusClient
#         Used to resolve the full variable list for a spec.
#     """

#     BASE = "https://api.census.gov/data"

#     def __init__(self, client):
#         self.client = client
#         self._session = requests.Session()
#         self._group_metadata_cache: dict[str, dict] = {}
#         self._abbrevs = {k.lower(): v for k, v in ABBREVIATIONS.items()}

#     # ------------------------------------------------------------------ #
#     #  Public API
#     # ------------------------------------------------------------------ #

#     def build_mapping(
#         self,
#         spec: CensusDatasetSpec,
#         vintage: int | None = None,
#     ) -> dict[str, str]:
#         """
#         Build a variable_code → column_name mapping for a spec.

#         Parameters
#         ----------
#         spec : CensusDatasetSpec
#         vintage : int, optional
#             Defaults to the most recent vintage in the spec.

#         Returns
#         -------
#         dict[str, str]
#         """
#         vintage = vintage or max(spec.vintages)
#         variables = self.client.resolve_all_variables(spec, vintage)

#         # Fetch metadata per group
#         group_meta: dict[str, dict] = {}
#         for group in spec.groups:
#             group_meta[group] = self._fetch_group_metadata(
#                 spec.dataset, vintage, group
#             )

#         # Assign each variable to its group
#         vars_by_group: dict[str, list[str]] = defaultdict(list)
#         for var in variables:
#             assigned = False
#             for group, meta in group_meta.items():
#                 if var in meta["labels"]:
#                     vars_by_group[group].append(var)
#                     assigned = True
#                     break
#             if not assigned:
#                 vars_by_group["_ungrouped"].append(var)

#         # Pass 1: group prefixes from concept
#         group_prefixes: dict[str, str] = {}
#         for group, meta in group_meta.items():
#             group_prefixes[group] = self._compress_concept(meta["concept"])
#         group_prefixes["_ungrouped"] = "misc"

#         # Pass 2: build label tree per group, find distinguishing tokens
#         mapping: dict[str, str] = {}
#         for group, group_vars in vars_by_group.items():
#             labels = group_meta.get(group, {}).get("labels", {})
#             parsed = self._parse_labels(group_vars, labels)
#             token_map = self._find_distinguishing_tokens(parsed)

#             prefix = group_prefixes[group]
#             for var in group_vars:
#                 tokens = token_map.get(var, [])
#                 mapping[var] = self._build_column_name(var, prefix, tokens)

#         return mapping

#     def generate_staging_model(
#         self,
#         spec: CensusDatasetSpec,
#         mapping: dict[str, str],
#         output_dir: str,
#     ) -> str:
#         """
#         Write a dbt staging .sql file that aliases raw Census columns.

#         Returns the path to the written file.
#         """
#         geo_columns = GEOGRAPHY_CONFIG[spec.geography_level]["geo_columns"]
#         geo_cols_sanitized = [
#             self.client._sanitize_column_name(c) for c in geo_columns
#         ]

#         source_ref = (
#             f"{{{{ source('{spec.target_schema}', '{spec.target_table}') }}}}"
#         )

#         select_lines = []
#         for col in geo_cols_sanitized:
#             select_lines.append(f'        "{col}"')
#         select_lines.append('        "vintage"')
#         select_lines.append('        "NAME"')
#         for var_code, col_name in sorted(mapping.items()):
#             select_lines.append(f'        "{var_code}" as "{col_name}"')

#         select_body = ",\n".join(select_lines)

#         sql = (
#             f"with aliased as (\n"
#             f"    select\n"
#             f"{select_body}\n"
#             f"    from {source_ref}\n"
#             f")\n"
#             f"\n"
#             f"select * from aliased\n"
#         )

#         os.makedirs(output_dir, exist_ok=True)
#         filepath = os.path.join(output_dir, f"{spec.name}.sql")
#         with open(filepath, "w") as f:
#             f.write(sql)

#         logger.info("Wrote staging model to %s (%d columns)", filepath, len(mapping))
#         return filepath

#     # ------------------------------------------------------------------ #
#     #  Metadata fetching
#     # ------------------------------------------------------------------ #

#     def _fetch_group_metadata(
#         self,
#         dataset: str,
#         vintage: int,
#         group: str,
#     ) -> dict:
#         """
#         Fetch and cache a group's metadata.

#         Returns dict with "concept" (str) and "labels" (dict[var_code, label]).

#         Note: the Census API stores the concept on each variable entry,
#         not at the top level of the group JSON.
#         """
#         cache_key = f"{dataset}/{vintage}/{group}"
#         if cache_key in self._group_metadata_cache:
#             return self._group_metadata_cache[cache_key]

#         url = f"{self.BASE}/{vintage}/{dataset}/groups/{group}.json"
#         resp = self._session.get(url)
#         resp.raise_for_status()
#         data = resp.json()

#         variables_raw = data.get("variables", {})

#         concept = group  # fallback
#         labels: dict[str, str] = {}
#         for name, info in variables_raw.items():
#             if name == group or name == "NAME":
#                 continue
#             labels[name] = info.get("label", "")
#             if concept == group:
#                 var_concept = info.get("concept", "")
#                 if var_concept:
#                     concept = var_concept

#         result = {"concept": concept, "labels": labels}
#         self._group_metadata_cache[cache_key] = result
#         return result

#     # ------------------------------------------------------------------ #
#     #  Pass 1: concept → group prefix
#     # ------------------------------------------------------------------ #

#     def _compress_concept(self, concept: str) -> str:
#         """Compress a group concept into a short prefix (max GROUP_PREFIX_MAX_LEN chars)."""
#         tokens = self._get_content_tokens(concept)
#         result = "_".join(tokens)
#         if len(result) > GROUP_PREFIX_MAX_LEN:
#             result = result[:GROUP_PREFIX_MAX_LEN].rstrip("_")
#         return result

#     # ------------------------------------------------------------------ #
#     #  Pass 2: label tree → distinguishing tokens
#     # ------------------------------------------------------------------ #

#     def _parse_labels(
#         self,
#         variables: list[str],
#         labels: dict[str, str],
#     ) -> dict[str, list[str]]:
#         """
#         Parse Census labels into cleaned segment lists, stripping
#         Estimate/MOE prefix and leading Total: segment.
#         """
#         parsed: dict[str, list[str]] = {}
#         for var in variables:
#             label = labels.get(var, "")
#             label = re.sub(r"^Estimate!!", "", label)
#             label = re.sub(r"^Margin of Error!!", "", label)
#             label = re.sub(r"^Total:?(!|$)", "", label).lstrip("!")

#             segments = []
#             for seg in label.split("!!"):
#                 seg = seg.strip().rstrip(":")
#                 if seg:
#                     segments.append(seg)

#             parsed[var] = segments
#         return parsed

#     def _find_distinguishing_tokens(
#         self,
#         parsed: dict[str, list[str]],
#     ) -> dict[str, list[str]]:
#         """
#         For each variable, walk the label tree and produce a compact,
#         distinguishing list of abbreviated tokens.

#         Strategy:
#         - Distant branching ancestors: minimum tokens to distinguish
#           from siblings (typically 1 token).
#         - Immediate parent (last branching level): up to PARENT_SCOPE_TOKENS
#           to convey the category scope.
#         - Non-branching leaf beyond the last branch: up to PARENT_SCOPE_TOKENS.
#         - Category totals (variables with children): append "tot".
#         - Deduplicate consecutive tokens, protecting the final token.
#         """
#         # Detect category totals: variables whose segments are a prefix
#         # of another variable's segments.
#         is_category_total: dict[str, bool] = {}
#         for var, segs in parsed.items():
#             is_category_total[var] = any(
#                 len(other_segs) > len(segs)
#                 and other_segs[: len(segs)] == segs
#                 for other_var, other_segs in parsed.items()
#                 if other_var != var
#             )

#         result: dict[str, list[str]] = {}

#         for var, segs in parsed.items():
#             if not segs:
#                 result[var] = ["tot"]
#                 continue

#             # Identify branching depths and siblings at each
#             branching_levels: list[tuple[int, list[str]]] = []
#             for depth in range(len(segs)):
#                 prefix = tuple(segs[:depth])
#                 siblings: list[str] = []
#                 for other_segs in parsed.values():
#                     if (
#                         len(other_segs) > depth
#                         and tuple(other_segs[:depth]) == prefix
#                         and other_segs[depth] != segs[depth]
#                     ):
#                         if other_segs[depth] not in siblings:
#                             siblings.append(other_segs[depth])
#                 if siblings:
#                     branching_levels.append((depth, siblings))

#             tokens: list[str] = []

#             for i, (depth, siblings) in enumerate(branching_levels):
#                 is_last_branch = i == len(branching_levels) - 1

#                 if is_last_branch:
#                     # Immediate parent scope: more tokens to show category breadth
#                     tokens.extend(
#                         self._get_content_tokens(
#                             segs[depth], max_n=PARENT_SCOPE_TOKENS
#                         )
#                     )
#                 else:
#                     # Distant ancestor: minimum distinguishing tokens
#                     tokens.extend(
#                         self._min_distinguishing_tokens(segs[depth], siblings)
#                     )

#             # If the leaf is beyond the last branching depth (a non-branching
#             # leaf segment), add its content tokens
#             last_branch_depth = branching_levels[-1][0] if branching_levels else -1
#             leaf_depth = len(segs) - 1
#             if leaf_depth > last_branch_depth:
#                 tokens.extend(
#                     self._get_content_tokens(
#                         segs[leaf_depth], max_n=PARENT_SCOPE_TOKENS
#                     )
#                 )

#             if is_category_total[var]:
#                 tokens.append("tot")

#             if not tokens:
#                 tokens = ["tot"]

#             # Deduplicate consecutive tokens, always protecting the last
#             tokens = self._dedup_consecutive(tokens)

#             result[var] = tokens

#         return result

#     def _min_distinguishing_tokens(
#         self,
#         segment: str,
#         sibling_segments: list[str],
#     ) -> list[str]:
#         """
#         Find the minimum content tokens from segment that distinguish
#         it from all siblings.
#         """
#         my_tokens = self._get_content_tokens(segment)
#         if not my_tokens:
#             return []

#         sibling_token_lists = [
#             self._get_content_tokens(s) for s in sibling_segments
#         ]

#         for n in range(1, len(my_tokens) + 1):
#             candidate = my_tokens[:n]
#             if all(sib[:n] != candidate for sib in sibling_token_lists):
#                 return candidate

#         return my_tokens

#     # ------------------------------------------------------------------ #
#     #  Column name assembly
#     # ------------------------------------------------------------------ #

#     def _build_column_name(
#         self,
#         variable: str,
#         group_prefix: str,
#         tokens: list[str],
#     ) -> str:
#         """
#         Assemble: {group_prefix}__{tokens}_{est|moe}_{variable_code}

#         Truncates the token portion if the result exceeds 63 chars.
#         """
#         suffix_type = self._extract_suffix_type(variable)
#         leaf = "_".join(tokens) if tokens else "tot"
#         code = variable

#         full_name = f"{group_prefix}__{leaf}_{suffix_type}_{code}"

#         if len(full_name) > POSTGRES_MAX_IDENTIFIER:
#             reserved = f"__{suffix_type}_{code}"
#             max_leaf = POSTGRES_MAX_IDENTIFIER - len(group_prefix) - len(reserved) - 1
#             if max_leaf > 0:
#                 leaf_truncated = leaf[:max_leaf].rstrip("_")
#                 full_name = f"{group_prefix}__{leaf_truncated}_{suffix_type}_{code}"
#             else:
#                 full_name = f"{group_prefix}{reserved}"

#         return full_name.lower()

#     # ------------------------------------------------------------------ #
#     #  Text helpers
#     # ------------------------------------------------------------------ #

#     def _get_content_tokens(self, segment: str, max_n: int | None = None) -> list[str]:
#         """
#         Split a segment into words, abbreviate each, and return only
#         content tokens (skipping filler words like 'and', 'of', 'the').
#         """
#         words = re.split(r"[\s,;()\-/]+", segment)
#         tokens = []
#         for word in words:
#             if not word:
#                 continue
#             abbreviated = self._abbreviate_word(word)
#             if abbreviated and abbreviated not in FILLER_TOKENS:
#                 tokens.append(abbreviated)
#         if max_n:
#             tokens = tokens[:max_n]
#         return tokens

#     def _abbreviate_word(self, word: str) -> str:
#         """Look up a single word in the abbreviation dictionary."""
#         cleaned = re.sub(r"['\u2019]s$", "", word).strip(".:,;")
#         lookup = cleaned.lower()
#         if lookup in self._abbrevs:
#             return self._abbrevs[lookup]
#         if cleaned.isdigit():
#             return cleaned
#         return cleaned.lower()

#     @staticmethod
#     def _extract_suffix_type(variable: str) -> str:
#         """Extract est/moe from the variable code suffix."""
#         if variable.endswith("M") and not variable.endswith("PM"):
#             return "moe"
#         return "est"

#     @staticmethod
#     def _dedup_consecutive(tokens: list[str]) -> list[str]:
#         """
#         Remove consecutive duplicate tokens, but always protect the
#         final token (the leaf) from deduplication.
#         """
#         if len(tokens) <= 1:
#             return tokens
#         # Dedup the intermediate tokens
#         result = [tokens[0]]
#         for t in tokens[1:-1]:
#             if t != result[-1]:
#                 result.append(t)
#         # Always append the final token
#         result.append(tokens[-1])
#         return result

# """
# CensusVariableMapper — compresses Census Bureau variable labels into
# short, interpretable PostgreSQL column names (≤63 characters).

# Output format:
#     {concept_prefix}__{label_tokens}_{est|moe}_{variable_code}

# The variable_code suffix guarantees uniqueness; the prefix and label
# tokens provide human readability.

# Usage:
#     from census_metadata import CensusMetadata
#     from census_collector import CensusDatasetSpec
#     from variable_mapper import CensusVariableMapper

#     cm = CensusMetadata(api_key="YOUR_KEY")
#     spec = CensusDatasetSpec(
#         name="commute_by_race",
#         dataset="acs/acs5",
#         vintages=[2022],
#         groups=["B08105B"],
#         geography_level="tract",
#         target_table="commute_by_race_tract",
#         target_schema="raw_data",
#     )

#     mapper = CensusVariableMapper(metadata=cm)
#     enriched = mapper.build_mapping_df(spec, vintage=2022)
#     name_map  = mapper.build_column_map(spec, vintage=2022)
# """

# from __future__ import annotations

# import re

# import pandas as pd

# from loci.collectors.census.abbreviations import ABBREVIATIONS
# from loci.collectors.census.metadata import CensusMetadata
# from loci.collectors.census.spec import GEOGRAPHY_CONFIG, CensusDatasetSpec

# # Maximum PostgreSQL identifier length.
# PG_MAX_IDENTIFIER = 63

# # Variable suffixes we care about.  Annotations (EA, MA) are skipped.
# _SUFFIX_MAP = {
#     "E": "est",
#     "M": "moe",
# }

# # Tokens to drop entirely (articles, prepositions that add no meaning).
# _DROP_TOKENS = {"the", "of", "and", "or", "in", "to", "for", "by", "-"}


# def _classify_variable(variable: str) -> str | None:
#     """Return 'est', 'moe', or None (for annotations / unrecognized)."""
#     # Variable codes look like B24010_001E, B08105B_002MA
#     # The suffix after the last digit tells us the type.
#     m = re.search(r"\d+([A-Z]+)$", variable)
#     if not m:
#         return None
#     suffix = m.group(1)
#     return _SUFFIX_MAP.get(suffix)


# def _abbreviate_tokens(text: str) -> list[str]:
#     """
#     Tokenize text, abbreviate each token via the dictionary,
#     and drop filler words.

#     Returns a list of abbreviated, lowercased tokens.
#     """
#     # Strip colons, parens, commas; lowercase; split on whitespace.
#     cleaned = re.sub(r"[():,]", "", text).lower()
#     tokens = cleaned.split()
#     result = []
#     for tok in tokens:
#         if tok in _DROP_TOKENS:
#             continue
#         abbreviated = ABBREVIATIONS.get(tok, tok)
#         if abbreviated == "":
#             # Explicitly mapped to empty string (e.g., "means" -> "")
#             continue
#         result.append(abbreviated)
#     return result


# def _compress_concept(concept: str) -> str:
#     """Abbreviate and join concept tokens into a short prefix."""
#     tokens = _abbreviate_tokens(concept)
#     return "_".join(tokens)


# def _compress_label_segments(segments: list[str]) -> str:
#     """
#     Abbreviate each label segment (after splitting on '!!'),
#     skipping the first segment (Estimate/Margin of Error)
#     and 'Total' segments.

#     Returns underscore-joined abbreviated tokens.
#     """
#     tokens = []
#     for seg in segments:
#         seg_clean = seg.strip().rstrip(":")
#         if seg_clean.lower() in ("estimate", "margin of error", "total",
#                                   "annotation of estimate",
#                                   "annotation of margin of error"):
#             continue
#         tokens.extend(_abbreviate_tokens(seg_clean))
#     return "_".join(tokens)


# def _deduplicate_consecutive(tokens_str: str) -> str:
#     """Remove consecutive duplicate tokens in an underscore-joined string."""
#     parts = tokens_str.split("_")
#     deduped = [parts[0]] if parts else []
#     for p in parts[1:]:
#         if p != deduped[-1]:
#             deduped.append(p)
#     return "_".join(deduped)


# def _assemble_column_name(
#     concept_prefix: str,
#     label_compressed: str,
#     suffix: str,
#     variable_code: str,
# ) -> str:
#     """
#     Assemble the column name and truncate the concept prefix if needed
#     to stay within PG_MAX_IDENTIFIER.

#     Format: {concept_prefix}__{label}_{est|moe}_{variable_code}
#     """
#     # The fixed tail is:  __{label}_{suffix}_{code}
#     # If label is empty:  __{suffix}_{code}
#     if label_compressed:
#         tail = f"__{label_compressed}_{suffix}_{variable_code}"
#     else:
#         tail = f"__tot_{suffix}_{variable_code}"

#     tail = _deduplicate_consecutive(tail)
#     budget = PG_MAX_IDENTIFIER - len(tail)

#     if budget <= 0:
#         # Extreme case: even without a prefix we're over.  Use just the tail.
#         return tail.lstrip("_")[:PG_MAX_IDENTIFIER]

#     # Truncate at underscore boundaries so we don't cut words in half.
#     if len(concept_prefix) > budget:
#         prefix = concept_prefix[:budget]
#         # Back up to the last underscore so we don't leave a partial token.
#         last_sep = prefix.rfind("_")
#         prefix = prefix[:last_sep] if last_sep > 0 else prefix
#     else:
#         prefix = concept_prefix
#     prefix = prefix.rstrip("_")

#     full = f"{prefix}{tail}"
#     full = _deduplicate_consecutive(full)
#     return full[:PG_MAX_IDENTIFIER]


# class CensusVariableMapper:
#     """
#     Compresses Census variable labels into readable PostgreSQL column names.

#     Parameters
#     ----------
#     metadata : CensusMetadata
#         Used to fetch variable labels and concepts from the Census API.
#     extra_abbreviations : dict[str, str], optional
#         Additional abbreviations to merge with the default dictionary.
#     """

#     def __init__(
#         self,
#         metadata: CensusMetadata,
#         extra_abbreviations: dict[str, str] | None = None,
#     ):
#         self._metadata = metadata
#         self._abbreviations = dict(ABBREVIATIONS)
#         if extra_abbreviations:
#             self._abbreviations.update(extra_abbreviations)

#     def _fetch_variables(self, spec: CensusDatasetSpec, vintage: int) -> pd.DataFrame:
#         """
#         Fetch variable metadata for all groups and individual variables
#         defined in the spec.
#         """
#         frames = []
#         for group in spec.groups:
#             df = self._metadata.list_variables(spec.dataset, vintage, group=group)
#             frames.append(df)
#         if spec.variables:
#             # Fetch the full variable list and filter to the ones requested.
#             all_vars = self._metadata.list_variables(spec.dataset, vintage)
#             frames.append(all_vars[all_vars["variable"].isin(spec.variables)])
#         if not frames:
#             return pd.DataFrame(columns=["variable", "label", "group", "concept", "predicate_type"])
#         return pd.concat(frames, ignore_index=True).drop_duplicates(subset="variable")

#     def build_mapping_df(self, spec: CensusDatasetSpec, vintage: int) -> pd.DataFrame:
#         """
#         Fetch variable metadata for a spec and return it enriched with:
#           - var_type: 'est' or 'moe'
#           - concept_compressed: abbreviated concept string
#           - label_compressed: abbreviated label string (without concept/type prefix)
#           - column_name: the final compressed column name

#         Rows with annotations (EA, MA) or unrecognized suffixes are excluded.
#         """
#         variables_df = self._fetch_variables(spec, vintage)
#         df = variables_df.copy()

#         df["var_type"] = df["variable"].apply(_classify_variable)
#         df = df[df["var_type"].notna()].copy()

#         df["concept_compressed"] = df["concept"].apply(_compress_concept)

#         df["label_compressed"] = df["label"].apply(
#             lambda lbl: _compress_label_segments(lbl.split("!!"))
#         )

#         df["column_name"] = df.apply(
#             lambda row: _assemble_column_name(
#                 row["concept_compressed"],
#                 row["label_compressed"],
#                 row["var_type"],
#                 row["variable"],
#             ),
#             axis=1,
#         )

#         return df.reset_index(drop=True)

#     def build_column_map(self, spec: CensusDatasetSpec, vintage: int) -> dict[str, str]:
#         """
#         Return a dict mapping original variable codes to compressed column names.

#         {variable_code: column_name}

#         Suitable for use in a dbt model's column aliasing.
#         """
#         df = self.build_mapping_df(spec, vintage)
#         return dict(zip(df["variable"], df["column_name"]))


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
            f"with renamed as (\n"
            f"    select\n"
            f"{select_clause}\n"
            f"    from {source_ref}\n"
            f")\n"
            f"\n"
            f"select * from renamed\n"
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
