from __future__ import annotations

import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path

import requests
from loci.collectors.tiger.metadata import TIGER_LAYER_SCOPE, TigerMetadata

logger = logging.getLogger(__name__)


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


@dataclass
class TigerDatasetSpec:
    """
    Defines a TIGER/Line or Cartographic Boundary dataset to collect.

    Parameters
    ----------
    name : str
        Human-readable name (e.g. "census_tracts", "primary_roads").
    layer : str
        TIGER layer name (e.g. "TRACT", "BG", "PRIMARYROADS", "ROADS").
        Case-insensitive; will be uppercased for TIGER, lowercased for
        cartographic.
    vintages : list[int]
        Years to collect (e.g. [2023, 2024]).
    target_table : str
        Destination table name (e.g. "census_tracts").
    target_schema : str
        Destination schema name (e.g. "raw_data").
    source : str
        "tiger" for TIGER/Line shapefiles, "cartographic" for
        Cartographic Boundary files. Default "tiger".
    resolution : str
        For cartographic files only. Default "500k".
    state_fips : list[str] | None
        Specific state FIPS codes. None means all states.
        Ignored for national-scope layers.
    entity_key : list[str] | None
        Columns that uniquely identify a feature (used for SCD2 merge).
        If None, a default is chosen based on common TIGER conventions.
    lowercase_columns : bool
        Whether to lowercase shapefile column names. Default True.
    """

    name: str
    layer: str
    vintages: list[int]
    target_table: str
    target_schema: str = "raw_data"
    source: str = "tiger"
    resolution: str = "500k"
    state_fips: list[str] | None = None
    entity_key: list[str] | None = None
    lowercase_columns: bool = True

    def __post_init__(self):
        if self.source not in ("tiger", "cartographic"):
            raise ValueError(f"Unknown source {self.source!r}. Use 'tiger' or 'cartographic'.")

    @property
    def scope(self) -> str:
        """Return the geographic scope: 'national', 'state', or 'county'."""
        layer_upper = self.layer.upper()
        return TIGER_LAYER_SCOPE.get(layer_upper, "state")

    @property
    def states(self) -> list[str]:
        return self.state_fips if self.state_fips else ALL_STATE_FIPS


# Fiona type prefix -> PostGIS type mapping.
# Fiona reports types like 'str:80', 'int:10', 'float:24.15', 'date', etc.
FIONA_TO_PG_TYPE = {
    "str": "text",
    "int": "bigint",
    "int32": "integer",
    "int64": "bigint",
    "float": "double precision",
    "date": "date",
    "datetime": "timestamptz",
    "time": "time",
    "bytes": "bytea",
}

# Fiona geometry type -> PostGIS geometry type.
# We always promote to Multi variants because shapefiles often contain
# a mix of single and multi geometries (e.g. a Polygon shapefile may
# have some MultiPolygon features). PostGIS accepts single geometries
# into Multi columns but not vice versa.
FIONA_GEOM_TO_PG = {
    "Point": "geometry(MultiPoint, 4326)",
    "MultiPoint": "geometry(MultiPoint, 4326)",
    "LineString": "geometry(MultiLineString, 4326)",
    "MultiLineString": "geometry(MultiLineString, 4326)",
    "Polygon": "geometry(MultiPolygon, 4326)",
    "MultiPolygon": "geometry(MultiPolygon, 4326)",
    "3D Point": "geometry(MultiPointZ, 4326)",
    "3D MultiPoint": "geometry(MultiPointZ, 4326)",
    "3D LineString": "geometry(MultiLineStringZ, 4326)",
    "3D MultiLineString": "geometry(MultiLineStringZ, 4326)",
    "3D Polygon": "geometry(MultiPolygonZ, 4326)",
    "3D MultiPolygon": "geometry(MultiPolygonZ, 4326)",
}


def generate_tiger_ddl(
    spec: TigerDatasetSpec,
    vintage: int | None = None,
    geometry_column: str = "geom",
    srid: int = 4326,
) -> str:
    """
    Generate a CREATE TABLE statement for a TigerDatasetSpec.

    Downloads a sample shapefile, reads its schema via fiona, and
    produces DDL with appropriate column types, SCD2 columns, and
    constraints.

    Parameters
    ----------
    spec : TigerDatasetSpec
    vintage : int, optional
        The vintage to use for the sample file. Defaults to the most
        recent vintage in the spec.
    geometry_column : str
        Name for the geometry column. Default "geom".
    srid : int
        SRID to use in the geometry column type. Default 4326.

    Returns
    -------
    str
        A CREATE TABLE SQL statement with constraints and indexes.
    """
    vintage = vintage or max(spec.vintages)

    schema, geom_type = _inspect_sample_schema(spec, vintage)
    columns = _schema_to_columns(
        schema,
        geom_type,
        geometry_column,
        srid,
        spec.lowercase_columns,
    )
    entity_key = spec.entity_key or _default_entity_key(spec)

    fqn = f"{spec.target_schema}.{spec.target_table}"

    lines = [f"create table {fqn} ("]

    # Property columns
    for col_name, pg_type in columns:
        lines.append(f'    "{col_name}" {pg_type},')

    # Vintage
    lines.append('    "vintage" integer not null,')

    # Geometry column
    pg_geom_type = _geom_pg_type(geom_type, srid)
    lines.append(f'    "{geometry_column}" {pg_geom_type},')

    # Metadata / SCD2 columns
    lines.append("    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "record_hash" text not null,')
    lines.append("    \"valid_from\" timestamptz not null default (now() at time zone 'UTC'),")
    lines.append('    "valid_to" timestamptz')

    lines.append(");")

    # Unique constraint on entity key + record_hash
    ek_cols = ", ".join(f'"{c}"' for c in entity_key)
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

    # Spatial index
    spatial_index_name = f"ix_{spec.target_table}_{geometry_column}"
    lines.append("")
    lines.append(f"create index {spatial_index_name}")
    lines.append(f'    on {fqn} using gist ("{geometry_column}");')

    return "\n".join(lines)


def _inspect_sample_schema(
    spec: TigerDatasetSpec,
    vintage: int,
) -> tuple[dict, str]:
    """
    Download a sample shapefile and return its (schema_properties, geometry_type).

    For state-based layers, downloads the first state in the spec.
    For national layers, downloads the national file.
    """
    import fiona

    metadata = TigerMetadata()

    if spec.scope == "national":
        state_fips = None
    else:
        state_fips = spec.states[0]

    url = metadata.get_download_url(
        vintage=vintage,
        layer=spec.layer,
        source=spec.source,
        state_fips=state_fips,
        resolution=spec.resolution,
    )

    logger.info("Downloading sample shapefile from %s", url)
    filepath = _download_to_tempfile(url)

    try:
        path_str = f"zip://{filepath}"
        with fiona.open(path_str, "r") as src:
            schema = dict(src.schema.get("properties", {}))
            geom_type = src.schema.get("geometry", "Polygon")
            logger.info(
                "Inspected schema: %d properties, geometry=%s",
                len(schema),
                geom_type,
            )
            return schema, geom_type
    finally:
        filepath.unlink(missing_ok=True)


def _schema_to_columns(
    schema: dict,
    geom_type: str,
    geometry_column: str,
    srid: int,
    lowercase: bool,
) -> list[tuple[str, str]]:
    """
    Convert a fiona schema properties dict to a list of (column_name, pg_type)
    tuples.
    """
    columns = []
    for prop_name, fiona_type in schema.items():
        col_name = prop_name.lower() if lowercase else prop_name
        pg_type = _fiona_type_to_pg(fiona_type)
        columns.append((col_name, pg_type))
    return columns


def _fiona_type_to_pg(fiona_type: str) -> str:
    """
    Convert a fiona type string to a PostgreSQL type.

    Fiona types look like 'str:80', 'int:10', 'float:24.15', 'date', etc.
    We strip the width/precision suffix and map to PG types.
    """
    base_type = fiona_type.split(":")[0]
    return FIONA_TO_PG_TYPE.get(base_type, "text")


def _geom_pg_type(geom_type: str, srid: int) -> str:
    """Convert a fiona geometry type string to a PostGIS column type."""
    pg_type = FIONA_GEOM_TO_PG.get(geom_type)
    if pg_type and srid != 4326:
        # Replace the default 4326 with the requested SRID
        pg_type = pg_type.replace("4326", str(srid))
    return pg_type or f"geometry({geom_type}, {srid})"


def _default_entity_key(spec: TigerDatasetSpec) -> list[str]:
    """Return a default entity key for the spec."""
    geoid_col = "geoid" if spec.lowercase_columns else "GEOID"
    return [geoid_col, "vintage"]


def _download_to_tempfile(url: str) -> Path:
    """Download a URL to a temp file. Returns the file path."""
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    tmp = tempfile.NamedTemporaryFile(
        suffix=".zip",
        prefix="tiger_ddl_",
        delete=False,
    )
    try:
        for chunk in resp.iter_content(chunk_size=8192):
            tmp.write(chunk)
        tmp.close()
        return Path(tmp.name)
    except Exception:
        tmp.close()
        Path(tmp.name).unlink(missing_ok=True)
        raise
