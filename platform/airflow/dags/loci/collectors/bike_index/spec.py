"""
BikeIndexDatasetSpec — defines a Bike Index dataset to collect.

Usage:
    from bike_index.spec import BikeIndexDatasetSpec

    spec = BikeIndexDatasetSpec(
        name="chicago_stolen_bikes",
        target_table="stolen_bikes",
        location="Chicago, IL",
        distance=10,
        entity_key=["bike_index_id"],
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec


@dataclass
class BikeIndexDatasetSpec(DatasetSpec):
    """
    Defines a Bike Index stolen-bike dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "chicago_stolen_bikes").
    target_table : str
        Destination table name.
    target_schema : str
        Destination schema name. Default "raw_data".
    entity_key : list[str] | None
        Columns that uniquely identify a record for SCD2 merge.
    location : str
        City name, zip code, address, or "lat,lon".
    distance : int
        Radius in miles from location.
    stolenness : str
        One of "proximity", "stolen", "non", "all".
    query : str | None
        Optional free-text search (brand, model, color, etc.).
    per_page : int
        Results per page (max 100).
    fetch_detail : bool
        If True, make a second API call per bike to get lat/lon
        from the stolen_record. False for faster collection
        without coordinates.
    """

    name: str
    target_table: str
    target_schema: str = "raw_data"
    entity_key: list[str] | None = field(default_factory=lambda: ["id"])
    location: str = "Chicago, IL"
    distance: int = 10
    stolenness: str = "proximity"
    query: str | None = None
    per_page: int = 100
    fetch_detail: bool = True

    def __post_init__(self):
        if self.stolenness not in ("proximity", "stolen", "non", "all"):
            raise ValueError(
                f"stolenness must be 'proximity', 'stolen', 'non', or 'all', "
                f"got {self.stolenness!r}"
            )

    def to_search_params(self) -> dict:
        """Return a dict of query params for the Bike Index search endpoint."""
        params = {
            "location": self.location,
            "distance": self.distance,
            "stolenness": self.stolenness,
            "per_page": self.per_page,
        }
        if self.query:
            params["query"] = self.query
        return params
