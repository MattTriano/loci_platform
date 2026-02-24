from __future__ import annotations

import logging
import time

import requests
from loci.collectors.census.spec import GEOGRAPHY_CONFIG, MAX_VARIABLES_PER_CALL, CensusDatasetSpec

logger = logging.getLogger(__name__)


class CensusClient:
    """
    Makes Census Bureau API calls.

    Handles:
    - Resolving groups to their constituent variables
    - Chunking variable lists to stay within the 48-variable API limit
    - Joining chunked results back together
    """

    BASE = "https://api.census.gov/data"

    def __init__(self, api_key: str, requests_per_second: float = 5.0):
        self.api_key = api_key
        self._session = requests.Session()
        self._min_interval = 1.0 / requests_per_second
        self._last_request_time = 0.0
        self.logger = logging.getLogger("census_client")

    def _throttle(self):
        """Simple rate limiter."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _get_json(self, url: str, params: dict | None = None) -> list[list[str]]:
        """Fetch JSON from the Census API. Returns the raw list-of-lists response."""
        self._throttle()
        params = params or {}
        params["key"] = self.api_key
        resp = self._session.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def resolve_group_variables(self, dataset: str, vintage: int, group: str) -> list[str]:
        """
        Fetch the list of estimate variables in a group.

        Filters to actual data variables (ending in E, M, PE, PM, etc.)
        and excludes annotation variables.
        """
        url = f"{self.BASE}/{vintage}/{dataset}/groups/{group}.json"
        data = self._get_json(url)
        variables = data.get("variables", {})
        # Keep actual data variables, skip the group's NAME entry and
        # annotation columns (which typically have predicateType == null)
        var_names = [
            name
            for name, info in variables.items()
            if name != group and name != "NAME" and info.get("predicateType") is not None
        ]
        return sorted(var_names)

    def _is_estimate_or_moe(self, variable: str) -> bool:
        """Return True if the variable is an estimate (E) or margin of error (M)."""
        # Census variable codes look like B24010_001E, B24010_001M, etc.
        # We want codes ending in E or M, but not EA, MA, PE, PM, etc.
        # Standard pattern: group_NNNX where X is the suffix letter(s).
        if variable.endswith("EA") or variable.endswith("MA"):
            return False
        if variable.endswith("PE") or variable.endswith("PM"):
            return False
        return variable.endswith("E") or variable.endswith("M")

    def _column_type(self, variable: str) -> str:
        """Return the Postgres column type for a census variable."""
        # Estimates and MOEs are numeric values. The Census API returns them
        # as strings, but they represent counts, dollar amounts, percentages,
        # etc. — all numeric. Using numeric (arbitrary precision) avoids
        # floating-point surprises with dollar values and percentages.
        return "numeric"

    def _sanitize_column_name(self, name: str) -> str:
        """Replace spaces with underscores for use as a column name."""
        return name.replace(" ", "_")

    def resolve_all_variables(self, spec: CensusDatasetSpec, vintage: int) -> list[str]:
        """
        Resolve all groups + individual variables for a spec into a
        deduplicated, sorted variable list.

        Filters to estimate (E) and margin-of-error (M) variables only,
        excluding annotations (EA, MA) and percentages (PE, PM).
        """
        all_vars = set(spec.variables)
        for group in spec.groups:
            group_vars = self.resolve_group_variables(spec.dataset, vintage, group)
            self.logger.info(
                "Group %s resolved to %d variables for %s/%d",
                group,
                len(group_vars),
                spec.dataset,
                vintage,
            )
            all_vars.update(group_vars)
        return sorted(v for v in all_vars if self._is_estimate_or_moe(v))

    def fetch_variables(
        self,
        dataset: str,
        vintage: int,
        variables: list[str],
        geography_level: str,
        state_fips: str,
    ) -> list[dict[str, str]]:
        """
        Fetch census data for a list of variables, one state at a time.

        If the variable list exceeds MAX_VARIABLES_PER_CALL, splits into
        chunks and joins the results on the geography columns.

        Returns a list of dicts (one per geography row).
        """
        geo_config = GEOGRAPHY_CONFIG[geography_level]
        geo_columns = geo_config["geo_columns"]
        for_clause = geo_config["for"]

        # For state-level queries, we don't need "in" — the state IS the "for"
        if geography_level == "state":
            in_clause = None
            for_clause = f"state:{state_fips}"
        # ZCTA queries don't support "in state" filtering in most datasets
        elif geography_level == "zip code tabulation area":
            in_clause = None
        else:
            in_clause = f"state:{state_fips}"

        chunks = self._chunk_variables(variables)
        self.logger.info(
            "Fetching %d variables in %d chunk(s) for state %s, %s/%d [%s]",
            len(variables),
            len(chunks),
            state_fips,
            dataset,
            vintage,
            geography_level,
        )

        # Fetch the first chunk (this gives us the full row set)
        merged = self._fetch_chunk(
            dataset,
            vintage,
            chunks[0],
            for_clause,
            in_clause,
        )

        if not merged:
            return []

        # Fetch remaining chunks and join on geography columns
        for chunk in chunks[1:]:
            chunk_rows = self._fetch_chunk(
                dataset,
                vintage,
                chunk,
                for_clause,
                in_clause,
            )
            chunk_lookup = {tuple(row[c] for c in geo_columns): row for row in chunk_rows}
            for row in merged:
                key = tuple(row[c] for c in geo_columns)
                extra = chunk_lookup.get(key, {})
                for var in chunk:
                    row[var] = extra.get(var)

        return merged

    def _fetch_chunk(
        self,
        dataset: str,
        vintage: int,
        variables: list[str],
        for_clause: str,
        in_clause: str | None,
    ) -> list[dict[str, str]]:
        """Fetch a single chunk of variables from the API."""
        get_param = ",".join(["NAME"] + variables)
        url = f"{self.BASE}/{vintage}/{dataset}"
        params = {"get": get_param, "for": for_clause}
        if in_clause:
            params["in"] = in_clause

        try:
            raw = self._get_json(url, params)
        except requests.HTTPError as e:
            self.logger.error(
                "Census API error for %s/%d: %s",
                dataset,
                vintage,
                e,
            )
            raise

        if not raw or len(raw) < 2:
            return []

        header = raw[0]
        rows = []
        for row_data in raw[1:]:
            rows.append(dict(zip(header, row_data)))
        return rows

    @staticmethod
    def _chunk_variables(variables: list[str]) -> list[list[str]]:
        """Split a variable list into chunks of MAX_VARIABLES_PER_CALL."""
        return [
            variables[i : i + MAX_VARIABLES_PER_CALL]
            for i in range(0, len(variables), MAX_VARIABLES_PER_CALL)
        ]
