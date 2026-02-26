# """
# OsmDatasetSpec — defines what OSM data to collect from a Geofabrik PBF extract.

# One spec = one element type = one target table, matching the parse_pbf interface.

# Usage:
#     from osm_spec import OsmDatasetSpec

#     spec = OsmDatasetSpec(
#         name="illinois_nodes",
#         region_id="illinois",
#         element_type="nodes",
#         target_table="osm_nodes",
#         target_schema="raw_data",
#     )

#     # For ways, you may want to tune the node location cache:
#     spec = OsmDatasetSpec(
#         name="illinois_ways",
#         region_id="illinois",
#         element_type="ways",
#         target_table="osm_ways",
#         target_schema="raw_data",
#         location_storage="sparse_file_array,/tmp/osm_node_cache.nodecache",
#     )
# """

# from __future__ import annotations

# from dataclasses import dataclass

# VALID_ELEMENT_TYPES = ("nodes", "ways", "relations")


# @dataclass
# class OsmDatasetSpec:
#     """
#     Defines a single OSM element type to collect from a Geofabrik extract.

#     Parameters
#     ----------
#     name : str
#         Human-readable name (e.g. "illinois_nodes").
#     region_id : str
#         Geofabrik region ID (e.g. "illinois", "germany", "north-america").
#         Used to look up the PBF download URL via OsmMetadata.
#     element_type : str
#         One of "nodes", "ways", "relations".
#     target_table : str
#         Destination table name (e.g. "osm_nodes").
#     target_schema : str
#         Destination schema name (e.g. "raw_data").
#     geometry_column : str
#         Name for the geometry column. Default "geom".
#     srid : int
#         Spatial reference ID. Default 4326.
#     batch_size : int
#         Rows per batch yielded by the parser. Default 5000.
#     location_storage : str
#         pyosmium index type for caching node locations when parsing ways.
#         Default "flex_mem". For large files, use a disk-backed store like
#         "sparse_file_array,/tmp/osm_node_cache.nodecache".
#         Ignored for nodes and relations.
#     pbf_url : str | None
#         Explicit PBF download URL. If set, overrides region_id lookup.
#         Useful for custom extracts not in the Geofabrik index.
#     """

#     name: str
#     region_id: str
#     element_type: str
#     target_table: str
#     target_schema: str = "raw_data"
#     geometry_column: str = "geom"
#     srid: int = 4326
#     batch_size: int = 50000
#     location_storage: str = "flex_mem"
#     pbf_url: str | None = None

#     def __post_init__(self):
#         if self.element_type not in VALID_ELEMENT_TYPES:
#             raise ValueError(
#                 f"element_type must be one of {VALID_ELEMENT_TYPES}, "
#                 f"got {self.element_type!r}"
#             )

#     @property
#     def entity_key(self) -> list[str]:
#         """Entity key for SCD2 merge: always [osm_id]."""
#         return ["osm_id"]

"""
OsmDatasetSpec — defines what OSM data to collect from Geofabrik PBF extracts.

One spec = one element type = one target table, matching the parse_pbf interface.
Supports multiple regions per spec.

Usage:
    from osm_spec import OsmDatasetSpec

    spec = OsmDatasetSpec(
        name="osm_nodes",
        region_ids=["us/illinois", "us/indiana"],
        element_type="nodes",
        target_table="osm_nodes",
        target_schema="raw_data",
    )

    # Single region still works:
    spec = OsmDatasetSpec(
        name="osm_ways",
        region_ids=["us/illinois"],
        element_type="ways",
        target_table="osm_ways",
        target_schema="raw_data",
        location_storage="sparse_file_array,/tmp/osm_node_cache.nodecache",
    )
"""

from __future__ import annotations

from dataclasses import dataclass

VALID_ELEMENT_TYPES = ("nodes", "ways", "relations")


@dataclass
class OsmDatasetSpec:
    """
    Defines a single OSM element type to collect from one or more
    Geofabrik extracts.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "osm_nodes").
    region_ids : list[str]
        Geofabrik region IDs (e.g. ["us/illinois", "us/indiana"]).
        Used to look up PBF download URLs via OsmMetadata.
    element_type : str
        One of "nodes", "ways", "relations".
    target_table : str
        Destination table name (e.g. "osm_nodes").
    target_schema : str
        Destination schema name (e.g. "raw_data").
    geometry_column : str
        Name for the geometry column. Default "geom".
    srid : int
        Spatial reference ID. Default 4326.
    batch_size : int
        Rows per batch yielded by the parser. Default 50000.
    location_storage : str
        pyosmium index type for caching node locations when parsing ways.
        Default "flex_mem". For large files, use a disk-backed store like
        "sparse_file_array,/tmp/osm_node_cache.nodecache".
        Ignored for nodes and relations.
    """

    name: str
    region_ids: list[str]
    element_type: str
    target_table: str
    target_schema: str = "raw_data"
    geometry_column: str = "geom"
    srid: int = 4326
    batch_size: int = 250_000
    location_storage: str = "flex_mem"

    def __post_init__(self):
        if self.element_type not in VALID_ELEMENT_TYPES:
            raise ValueError(
                f"element_type must be one of {VALID_ELEMENT_TYPES}, got {self.element_type!r}"
            )
        if not self.region_ids:
            raise ValueError("region_ids must contain at least one region.")

    @property
    def entity_key(self) -> list[str]:
        """Entity key for SCD2 merge: [osm_id, region_id]."""
        return ["osm_id", "region_id"]


# def generate_ddl(spec: OsmDatasetSpec) -> str:
#     """
#     Generate a CREATE TABLE statement for an OsmDatasetSpec.

#     Parameters
#     ----------
#     spec : OsmDatasetSpec

#     Returns
#     -------
#     str
#         A CREATE TABLE SQL statement.
#     """
#     fqn = f"{spec.target_schema}.{spec.target_table}"
#     geom_col = spec.geometry_column

#     lines = [f"create table {fqn} ("]
#     lines.append('    "osm_id" bigint not null,')
#     lines.append('    "tags" jsonb,')

#     if spec.element_type == "relations":
#         lines.append('    "members" jsonb,')
#     else:
#         # nodes get Point, ways get Geometry (mix of LineString/Polygon)
#         if spec.element_type == "nodes":
#             geom_type = f"geometry(Point, {spec.srid})"
#         else:
#             geom_type = f"geometry(Geometry, {spec.srid})"
#         lines.append(f'    "{geom_col}" {geom_type},')

#     # SCD2 metadata
#     lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
#     lines.append('    "record_hash" text not null,')
#     lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
#     lines.append('    "valid_to" timestamptz')
#     lines.append(");")

#     # Unique constraint on entity key + record_hash
#     ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
#     constraint_name = f"uq_{spec.target_table}_entity_hash"
#     lines.append("")
#     lines.append(f"alter table {fqn}")
#     lines.append(f"    add constraint {constraint_name}")
#     lines.append(f'    unique ({ek_cols}, "record_hash");')

#     # Partial index for current versions
#     index_name = f"ix_{spec.target_table}_current"
#     lines.append("")
#     lines.append(f"create index {index_name}")
#     lines.append(f"    on {fqn} ({ek_cols})")
#     lines.append('    where "valid_to" is null;')

#     # Spatial index (only for nodes and ways)
#     if spec.element_type != "relations":
#         spatial_index_name = f"ix_{spec.target_table}_{geom_col}"
#         lines.append("")
#         lines.append(f"create index {spatial_index_name}")
#         lines.append(f'    on {fqn} using gist ("{geom_col}");')

#     return "\n".join(lines)

# def generate_ddl(spec: OsmDatasetSpec) -> str:
#     """
#     Generate a CREATE TABLE statement for an OsmDatasetSpec.

#     Parameters
#     ----------
#     spec : OsmDatasetSpec

#     Returns
#     -------
#     str
#         A CREATE TABLE SQL statement.
#     """
#     fqn = f"{spec.target_schema}.{spec.target_table}"
#     geom_col = spec.geometry_column

#     lines = [f"create table {fqn} ("]
#     lines.append('    "osm_id" bigint not null,')
#     lines.append('    "region_id" text not null,')
#     lines.append('    "tags" jsonb,')

#     if spec.element_type == "relations":
#         lines.append('    "members" jsonb,')
#     else:
#         if spec.element_type == "nodes":
#             geom_type = f"geometry(Point, {spec.srid})"
#         else:
#             geom_type = f"geometry(Geometry, {spec.srid})"
#         lines.append(f'    "{geom_col}" {geom_type},')

#     # SCD2 metadata
#     lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
#     lines.append('    "record_hash" text not null,')
#     lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
#     lines.append('    "valid_to" timestamptz')
#     lines.append(");")

#     # Unique constraint on entity key + record_hash
#     ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
#     constraint_name = f"uq_{spec.target_table}_entity_hash"
#     lines.append("")
#     lines.append(f"alter table {fqn}")
#     lines.append(f"    add constraint {constraint_name}")
#     lines.append(f'    unique ({ek_cols}, "record_hash");')

#     # Partial index for current versions
#     index_name = f"ix_{spec.target_table}_current"
#     lines.append("")
#     lines.append(f"create index {index_name}")
#     lines.append(f"    on {fqn} ({ek_cols})")
#     lines.append('    where "valid_to" is null;')

#     # Spatial index (only for nodes and ways)
#     if spec.element_type != "relations":
#         spatial_index_name = f"ix_{spec.target_table}_{geom_col}"
#         lines.append("")
#         lines.append(f"create index {spatial_index_name}")
#         lines.append(f'    on {fqn} using gist ("{geom_col}");')

#     return "\n".join(lines)


def generate_ddl(spec: OsmDatasetSpec) -> str:
    """
    Generate a CREATE TABLE statement for an OsmDatasetSpec.

    Parameters
    ----------
    spec : OsmDatasetSpec

    Returns
    -------
    str
        A CREATE TABLE SQL statement.
    """
    fqn = f"{spec.target_schema}.{spec.target_table}"
    geom_col = spec.geometry_column

    lines = [f"create table {fqn} ("]
    lines.append('    "osm_id" bigint not null,')
    lines.append('    "region_id" text not null,')
    lines.append('    "tags" jsonb,')

    if spec.element_type == "relations":
        lines.append('    "members" jsonb,')
    else:
        if spec.element_type == "nodes":
            geom_type = f"geometry(Point, {spec.srid})"
        else:
            geom_type = f"geometry(Geometry, {spec.srid})"
        lines.append(f'    "{geom_col}" {geom_type},')

    lines.append('    "osm_timestamp" timestamptz,')
    lines.append('    "osm_version" integer,')
    lines.append('    "osm_changeset" bigint,')
    # SCD2 metadata
    lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "record_hash" text not null,')
    lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "valid_to" timestamptz')
    lines.append(");")

    # Unique constraint on entity key + record_hash
    ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
    constraint_name = f"uq_{spec.target_table}_entity_hash"
    lines.append("")
    lines.append(f"alter table {fqn}")
    lines.append(f"    add constraint {constraint_name}")
    lines.append(f'    unique ({ek_cols}, "record_hash");')

    # Partial index for current versions
    index_name = f"ix_{spec.target_table}_current"
    lines.append("")
    lines.append(f"create index {index_name}")
    lines.append(f"    on {fqn} ({ek_cols})")
    lines.append('    where "valid_to" is null;')

    # Spatial index (only for nodes and ways)
    if spec.element_type != "relations":
        spatial_index_name = f"ix_{spec.target_table}_{geom_col}"
        lines.append("")
        lines.append(f"create index {spatial_index_name}")
        lines.append(f'    on {fqn} using gist ("{geom_col}");')

    return "\n".join(lines)
