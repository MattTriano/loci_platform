"""
Serialize a 2D elevation array into a PostGIS raster hex-WKB string.

This is the one primitive that lets us ingest rasters through the
existing StagedIngest path: PostGIS's `raster` input function parses a
hex-WKB literal, exactly like `geometry` parses WKT. So we produce the
hex string here, hand it to staged_ingest as the value of a `raster`
column, and the COPY -> SCD2 merge path handles the rest unchanged.

Only the single-band, in-db case we need for DEMs is implemented. Every
band is emitted as 32-bit float (PT_32BF): elevation fits in float32
with room to spare, and emitting one pixel type keeps this simple and
keeps the SCD2 checksum stable across sources that happen to store
integers vs floats.

Format reference: PostGIS raster serialized WKB (rt_raster_serialize).
Layout, little-endian (NDR):

    uint8   endianness   1 = NDR (little-endian)
    uint16  version      0
    uint16  n_bands      1
    float64 scale_x
    float64 scale_y
    float64 ip_x         upper-left corner X of the upper-left pixel
    float64 ip_y         upper-left corner Y of the upper-left pixel
    float64 skew_x
    float64 skew_y
    int32   srid
    uint16  width
    uint16  height
    -- per band:
    uint8   band_header  bit6 (0x40) = has nodata; bits0-3 = pixel type
    float32 nodata       (sized to the pixel type; 32BF here)
    float32[width*height] pixel data, row-major (top row first)
"""

from __future__ import annotations

import struct

import numpy as np

# PostGIS rt_pixtype value for 32-bit float. We always emit this.
_PT_32BF = 10
_HAS_NODATA_FLAG = 0x40

# A nodata value to use when the source raster declares none. PostGIS
# requires *some* value in the band header; we set the flag off so it is
# never interpreted as nodata, but still write a placeholder.
_NO_NODATA_PLACEHOLDER = 0.0


def to_hexwkb(
    pixels: np.ndarray,
    *,
    scale_x: float,
    scale_y: float,
    ip_x: float,
    ip_y: float,
    srid: int,
    nodata: float | None,
    skew_x: float = 0.0,
    skew_y: float = 0.0,
) -> str:
    """
    Serialize a 2D array into a single-band 32BF PostGIS raster hex-WKB.

    Parameters
    ----------
    pixels : np.ndarray
        2D array, shape (height, width). Cast to float32 on the way out.
    scale_x, scale_y : float
        Pixel size in CRS units. scale_y is normally negative (north-up).
    ip_x, ip_y : float
        Upper-left corner of the upper-left pixel (the GDAL/affine origin).
    srid : int
        Spatial reference id of the raster's CRS.
    nodata : float | None
        Band nodata value, or None if the source declares none.
    skew_x, skew_y : float
        Rotation terms. Zero for north-up rasters.

    Returns
    -------
    str
        Hex-WKB string (lowercase, no leading 0x) suitable as the value
        of a `raster` column in a text-format COPY.
    """
    if pixels.ndim != 2:
        raise ValueError(f"pixels must be 2D (height, width); got shape {pixels.shape}")

    height, width = pixels.shape
    if width > 0xFFFF or height > 0xFFFF:
        raise ValueError(
            f"tile {width}x{height} exceeds the uint16 raster dimension limit (65535); "
            "use a smaller tile size"
        )

    has_nodata = nodata is not None
    nodata_value = float(nodata) if has_nodata else _NO_NODATA_PLACEHOLDER
    band_header = _PT_32BF | (_HAS_NODATA_FLAG if has_nodata else 0)

    out = bytearray()
    out += struct.pack("<B", 1)  # endianness: NDR
    out += struct.pack("<H", 0)  # version
    out += struct.pack("<H", 1)  # n_bands
    out += struct.pack("<dddddd", scale_x, scale_y, ip_x, ip_y, skew_x, skew_y)
    out += struct.pack("<i", srid)
    out += struct.pack("<H", width)
    out += struct.pack("<H", height)
    out += struct.pack("<B", band_header)
    out += struct.pack("<f", nodata_value)

    # Row-major, top row first. numpy C-order already matches; force
    # little-endian float32 so the byte layout is explicit, not
    # platform-dependent.
    out += np.ascontiguousarray(pixels, dtype="<f4").tobytes()

    return out.hex()
