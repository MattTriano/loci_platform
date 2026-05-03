"""Geographic bounding box.

A small named type for bounding boxes so callers can't silently swap
the lat/lon order. Use `BBox(south=..., west=..., north=..., east=...)`
to construct, or `BBox.from_sw_ne(sw=(lat, lon), ne=(lat, lon))` if you
have corner points instead.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BBox:
    """A geographic bounding box in lat/lon degrees.

    Defaults to EPSG:4269 (NAD83). Pass a different `srid` if needed.

    Attributes:
        south: Minimum latitude.
        west:  Minimum longitude.
        north: Maximum latitude.
        east:  Maximum longitude.
        srid:  Spatial reference system identifier.
    """

    south: float
    west: float
    north: float
    east: float
    srid: int = 4269

    def __post_init__(self) -> None:
        if not self.south < self.north:
            raise ValueError(
                f"south ({self.south}) must be less than north ({self.north}); "
                "did you swap lat/lon?"
            )
        if not self.west < self.east:
            raise ValueError(
                f"west ({self.west}) must be less than east ({self.east}); did you swap lat/lon?"
            )
        if not -90 <= self.south <= 90 and -90 <= self.north <= 90:
            raise ValueError(f"latitudes out of range: south={self.south}, north={self.north}")
        if not -180 <= self.west <= 180 and -180 <= self.east <= 180:
            raise ValueError(f"longitudes out of range: west={self.west}, east={self.east}")

    def to_st_makeenvelope(self) -> str:
        """Render as a PostGIS ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid) call."""
        return f"ST_MakeEnvelope({self.west}, {self.south}, {self.east}, {self.north}, {self.srid})"
