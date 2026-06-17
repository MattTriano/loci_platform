# /loci_platform/platform/airflow/dags/loci/geo.py
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
        if not (-90 <= self.south <= 90 and -90 <= self.north <= 90):
            raise ValueError(f"latitudes out of range: south={self.south}, north={self.north}")
        if not (-180 <= self.west <= 180 and -180 <= self.east <= 180):
            raise ValueError(f"longitudes out of range: west={self.west}, east={self.east}")
        if self.west > self.east:
            raise ValueError(
                f"longitudes possibly reversed, west > east: west={self.west}, east={self.east}"
            )
        if self.south > self.north:
            raise ValueError(
                f"latitudes possibly reversed, south > north: south={self.south}, north={self.north}"
            )

    def to_st_makeenvelope(self) -> str:
        """Render as a PostGIS ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid) call."""
        return f"ST_MakeEnvelope({self.west}, {self.south}, {self.east}, {self.north}, {self.srid})"


@dataclass(frozen=True)
class Gate:
    """A line segment used as a geometric route assertion.

    Endpoints are (lat, lon) in degrees, matching RouteTestCase's
    origin/destination convention. A route "crosses" the gate when one of
    its legs transversally intersects this segment — used to assert that a
    route passes through (must_cross) or avoids (must_not_cross) a specific
    corridor, independent of street names.
    """

    start: tuple[float, float]  # (lat, lon)
    end: tuple[float, float]  # (lat, lon)

    def __post_init__(self) -> None:
        for label, (lat, lon) in (("start", self.start), ("end", self.end)):
            if not -90.0 <= lat <= 90.0:
                raise ValueError(
                    f"Gate {label} latitude {lat} out of range [-90, 90]. "
                    f"Are the endpoints (lat, lon) rather than (lon, lat)?"
                )
            if not -180.0 <= lon <= 180.0:
                raise ValueError(f"Gate {label} longitude {lon} out of range [-180, 180].")
        if self.start == self.end:
            raise ValueError(
                f"Gate endpoints are identical ({self.start}); a gate must have length."
            )
