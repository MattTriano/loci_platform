# """
# Census data collection and ingestion pipeline.

# Classes:
#     CensusDatasetSpec  — defines what census data to collect
#     CensusClient       — makes Census Bureau API calls
#     CensusCollector    — orchestrates collection and ingestion to Postgres

# Usage:
#     from census_collector import CensusDatasetSpec, CensusClient, CensusCollector

#     spec = CensusDatasetSpec(
#         name="occupation_by_sex",
#         dataset="acs/acs5",
#         vintages=[2019, 2020, 2021, 2022],
#         groups=["B24010"],
#         variables=["B01001_001E"],
#         geography_level="tract",
#         target_table="occupation_by_sex_tract",
#         target_schema="raw_data",
#     )

#     client = CensusClient(api_key="YOUR_KEY")
#     collector = CensusCollector(client=client, engine=engine)
#     collector.collect(spec)
# """

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# The Census API allows 50 variables per call, but NAME and the geography
# identifier columns count toward that limit. We reserve a few slots for
# those, giving us 48 user-specified variables per request.
MAX_VARIABLES_PER_CALL = 48

# All 50 states + DC + PR
ALL_STATE_FIPS = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "72",
]

# Geography level -> Census API "for" clause and the geo ID columns returned
GEOGRAPHY_CONFIG = {
    "state": {
        "for": "state:*",
        "geo_columns": ["state"],
    },
    "county": {
        "for": "county:*",
        "geo_columns": ["state", "county"],
    },
    "tract": {
        "for": "tract:*",
        "geo_columns": ["state", "county", "tract"],
    },
    "block group": {
        "for": "block group:*",
        "geo_columns": ["state", "county", "tract", "block group"],
    },
    "place": {
        "for": "place:*",
        "geo_columns": ["state", "place"],
    },
    "zip code tabulation area": {
        "for": "zip code tabulation area:*",
        "geo_columns": ["zip code tabulation area"],
    },
}


# ------------------------------------------------------------------ #
#  Dataclass
# ------------------------------------------------------------------ #


@dataclass
class CensusDatasetSpec:
    """
    Defines a set of census variables/groups to collect across vintages.

    Parameters
    ----------
    name : str
        A human-readable name for this spec (e.g. "occupation_by_sex").
    dataset : str
        Census API dataset path (e.g. "acs/acs5").
    vintages : list[int]
        Years to collect (e.g. [2019, 2020, 2021, 2022]).
    geography_level : str
        One of: "state", "county", "tract", "block group", "place",
        "zip code tabulation area".
    target_table : str
        Destination table name (e.g. "occupation_by_sex_tract").
    target_schema : str
        Destination schema name (e.g. "raw_data").
    groups : list[str]
        Group codes to collect all variables from (e.g. ["B24010"]).
    variables : list[str]
        Individual variable codes (e.g. ["B01001_001E"]).
    state_fips : list[str] | None
        Specific state FIPS codes to collect. None means all states.
    """

    name: str
    dataset: str
    vintages: list[int]
    geography_level: str
    target_table: str
    target_schema: str
    groups: list[str] = field(default_factory=list)
    variables: list[str] = field(default_factory=list)
    state_fips: list[str] | None = None

    def __post_init__(self):
        if not self.groups and not self.variables:
            raise ValueError("Must specify at least one group or variable.")
        if self.geography_level not in GEOGRAPHY_CONFIG.keys():
            raise ValueError(
                f"Unknown geography_level {self.geography_level!r}. "
                f"Supported: {list(GEOGRAPHY_CONFIG.keys())}"
            )

    @property
    def states(self) -> list[str]:
        return self.state_fips if self.state_fips else ALL_STATE_FIPS
