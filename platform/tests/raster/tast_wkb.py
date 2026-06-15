"""
Unit tests for loci.raster.wkb.to_hexwkb.

These are pure and offline: we serialize, then decode the bytes back and
assert the structure and pixels round-trip. The decoder lives only in the
test — it is deliberately independent of the serializer so a bug in one
can't hide a bug in the other.
"""

from __future__ import annotations

import struct

import numpy as np
import pytest
from loci.raster.wkb import to_hexwkb


def decode(hexwkb: str) -> dict:
    """Minimal independent decoder for the single-band 32BF layout."""
    b = bytes.fromhex(hexwkb)
    off = 0
    (endian,) = struct.unpack_from("<B", b, off)
    off += 1
    (version,) = struct.unpack_from("<H", b, off)
    off += 2
    (nbands,) = struct.unpack_from("<H", b, off)
    off += 2
    sx, sy, ipx, ipy, skx, sky = struct.unpack_from("<dddddd", b, off)
    off += 48
    (srid,) = struct.unpack_from("<i", b, off)
    off += 4
    (w,) = struct.unpack_from("<H", b, off)
    off += 2
    (h,) = struct.unpack_from("<H", b, off)
    off += 2
    (band_header,) = struct.unpack_from("<B", b, off)
    off += 1
    (nodata,) = struct.unpack_from("<f", b, off)
    off += 4
    pixels = np.frombuffer(b, dtype="<f4", count=w * h, offset=off).reshape(h, w)
    consumed = off + w * h * 4
    return dict(
        endian=endian,
        version=version,
        nbands=nbands,
        scale_x=sx,
        scale_y=sy,
        ip_x=ipx,
        ip_y=ipy,
        skew_x=skx,
        skew_y=sky,
        srid=srid,
        width=w,
        height=h,
        pixtype=band_header & 0x0F,
        has_nodata=bool(band_header & 0x40),
        nodata=nodata,
        pixels=pixels,
        consumed=consumed,
        total=len(b),
    )


def _grid(h: int, w: int) -> np.ndarray:
    return np.arange(h * w, dtype=np.float32).reshape(h, w)


def test_header_fields_round_trip():
    d = decode(
        to_hexwkb(
            _grid(3, 5),
            scale_x=10.0,
            scale_y=-10.0,
            ip_x=440000.0,
            ip_y=4640000.0,
            srid=4269,
            nodata=-9999.0,
        )
    )
    assert d["endian"] == 1  # NDR / little-endian
    assert d["version"] == 0
    assert d["nbands"] == 1
    assert d["pixtype"] == 10  # PT_32BF
    assert (d["width"], d["height"]) == (5, 3)
    assert (d["scale_x"], d["scale_y"]) == (10.0, -10.0)
    assert (d["ip_x"], d["ip_y"]) == (440000.0, 4640000.0)
    assert d["srid"] == 4269


def test_pixels_round_trip_exactly():
    arr = _grid(7, 11)
    d = decode(
        to_hexwkb(arr, scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=None)
    )
    assert np.array_equal(d["pixels"], arr)


def test_no_trailing_or_missing_bytes():
    d = decode(
        to_hexwkb(
            _grid(13, 9), scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=0.0
        )
    )
    assert d["consumed"] == d["total"]


def test_nodata_flag_set_when_given():
    d = decode(
        to_hexwkb(
            _grid(2, 2), scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=-9999.0
        )
    )
    assert d["has_nodata"] is True
    assert d["nodata"] == -9999.0


def test_nodata_flag_clear_when_none():
    d = decode(
        to_hexwkb(
            _grid(2, 2), scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=None
        )
    )
    assert d["has_nodata"] is False


def test_skew_terms_preserved():
    d = decode(
        to_hexwkb(
            _grid(2, 2),
            scale_x=1.0,
            scale_y=-1.0,
            ip_x=0.0,
            ip_y=0.0,
            srid=4326,
            nodata=None,
            skew_x=0.5,
            skew_y=-0.25,
        )
    )
    assert d["skew_x"] == 0.5
    assert d["skew_y"] == -0.25


def test_non_square_dims_are_width_then_height():
    # A 2-row, 4-col tile must decode as width=4, height=2 (not transposed).
    d = decode(
        to_hexwkb(
            _grid(2, 4), scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=None
        )
    )
    assert (d["width"], d["height"]) == (4, 2)


def test_integer_input_is_cast_to_float32():
    arr = np.array([[1, 2], [3, 4]], dtype=np.int16)
    d = decode(
        to_hexwkb(arr, scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=None)
    )
    assert np.array_equal(d["pixels"], arr.astype(np.float32))


def test_large_elevation_values_keep_float32_precision():
    # Realistic elevations (and a deep nodata) survive the float32 round-trip.
    arr = np.array([[4421.0, -86.0], [1609.34, 0.0]], dtype=np.float32)
    d = decode(
        to_hexwkb(arr, scale_x=1.0, scale_y=-1.0, ip_x=0.0, ip_y=0.0, srid=4326, nodata=-999999.0)
    )
    assert np.array_equal(d["pixels"], arr)
    assert d["nodata"] == -999999.0


def test_rejects_non_2d():
    with pytest.raises(ValueError, match="2D"):
        to_hexwkb(
            np.zeros((2, 2, 1), dtype=np.float32),
            scale_x=1.0,
            scale_y=-1.0,
            ip_x=0.0,
            ip_y=0.0,
            srid=4326,
            nodata=None,
        )


def test_rejects_oversize_dimension():
    with pytest.raises(ValueError, match="65535"):
        to_hexwkb(
            np.zeros((1, 70000), dtype=np.float32),
            scale_x=1.0,
            scale_y=-1.0,
            ip_x=0.0,
            ip_y=0.0,
            srid=4326,
            nodata=None,
        )
