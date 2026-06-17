# /loci_platform/platform/airflow/dags/loci/collectors/osm/spec.py
"""
Dataset specification for OSM Overpass API collection.

An OSMSpec declares *what* OSM data to collect and *where* to store it:
the Overpass query, which tags to promote to typed columns, and the
target table identifiers.

Example:
    spec = OSMSpec(
        name="chicago_cafes",
        target_table="chicago_cafes",
        target_schema="raw_data",
        query=OverpassAPIQuery(
            element_types=["node", "way"],
            tag_filters=[{"amenity": "cafe"}],
            area_name="Chicago",
        ),
        promoted_tags=["name", "addr:street", "cuisine"],
    )
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from loci.collectors.base_spec import DatasetSpec
from loci.collectors.osm.query import OverpassAPIQuery


@dataclass
class OSMDatasetSpec(DatasetSpec):
    """
    Specification for an OSM Overpass API dataset.

    Parameters
    ----------
    name : str
        Human-readable dataset identifier. Conventionally similar or
        equal to target_table (e.g. "chicago_cafes").
    target_table : str
        Postgres table name in the target warehouse.
    target_schema : str
        Postgres schema name in the target warehouse.
    query : OverpassAPIQuery
        Declarative Overpass query describing what to fetch.
    promoted_tags : list[str]
        OSM tag keys to lift into typed text columns. Tag keys with
        characters that aren't valid in Postgres identifiers (e.g.
        "addr:street", "name:en") are auto-normalized for the column
        name; the original key is preserved for lookups against OSM
        responses. May be empty — all tags always go into the JSONB
        `tags` column regardless of what's promoted.
    entity_key : list[str]
        Defaults to ["osm_type", "osm_id"]. Overriding is unusual;
        a warning will be issued.

    Attributes
    ----------
    source : str
        Always "osm".
    tag_column_map : dict[str, str]
        Mapping from original OSM tag key (e.g. "addr:street") to
        normalized Postgres column name (e.g. "addr_street").
        Built in __post_init__ from promoted_tags.
    """

    source: str = field(default="osm", init=False)
    name: str = ""
    target_table: str = ""
    target_schema: str = "raw_data"
    query: OverpassAPIQuery | None = None
    promoted_tags: list[str] = field(default_factory=list)
    entity_key: list[str] = field(default_factory=lambda: ["osm_type", "osm_id"])

    # Populated in __post_init__
    tag_column_map: dict[str, str] = field(default_factory=dict, init=False)

    @property
    def dataset_id(self) -> str:
        return self.target_table

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name is required")
        if not self.target_table:
            raise ValueError("target_table is required")
        if not self.target_schema:
            raise ValueError("target_schema is required")
        if self.query is None:
            raise ValueError("query is required")

        if self.entity_key != ["osm_type", "osm_id"]:
            import logging

            logging.getLogger(__name__).warning(
                "OSMSpec %r overrides entity_key to %s; the standard key "
                "for OSM is ['osm_type', 'osm_id']. Make sure this is intentional.",
                self.name,
                self.entity_key,
            )

        self.tag_column_map = self._build_tag_column_map(self.promoted_tags)

    # ------------------------------------------------------------------
    # Tag-key -> column-name normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _build_tag_column_map(promoted_tags: list[str]) -> dict[str, str]:
        """
        Build an ordered map from original OSM tag keys to normalized
        Postgres column names.

        Normalization: lowercase, non-alphanumerics replaced with '_',
        leading/trailing '_' stripped. If the result starts with a digit
        or is empty, the tag key is rejected. If two different tag keys
        normalize to the same column name, raise — the spec author needs
        to disambiguate manually.
        """
        result: dict[str, str] = {}
        seen: dict[str, str] = {}  # normalized -> original (for collision reporting)

        for tag in promoted_tags:
            if not tag:
                raise ValueError("promoted_tags contains an empty string")

            normalized = re.sub(r"[^a-z0-9]+", "_", tag.lower()).strip("_")

            if not normalized:
                raise ValueError(f"Tag key {tag!r} normalizes to an empty column name.")
            if normalized[0].isdigit():
                raise ValueError(
                    f"Tag key {tag!r} normalizes to {normalized!r}, which "
                    f"starts with a digit and isn't a valid Postgres identifier. "
                    f"Rename it manually."
                )
            if normalized in seen:
                raise ValueError(
                    f"Tag keys {seen[normalized]!r} and {tag!r} both normalize "
                    f"to column name {normalized!r}. Rename one of them manually."
                )

            seen[normalized] = tag
            result[tag] = normalized

        return result

    @property
    def promoted_columns(self) -> list[str]:
        """Normalized column names for promoted tags, in declaration order."""
        return list(self.tag_column_map.values())
