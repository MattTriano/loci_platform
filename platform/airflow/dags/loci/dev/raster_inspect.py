"""
Inspect and render ingested elevation rasters.

The one rule for querying raster through PostgresEngine.query: never
select the `rast` column raw — it comes back as an opaque WKB blob.
Instead wrap it in a PostGIS raster function so the result is something
query() already understands:

  - scalars (ST_Value, ST_SummaryStats fields) -> ordinary DataFrame cols
  - ST_Envelope(rast) -> geometry -> query() returns a GeoDataFrame
  - ST_AsTIFF(...)     -> bytea    -> bytes you can open with rasterio

The functions below are thin wrappers around those patterns plus a
matplotlib renderer. Everything takes your live PostgresEngine.
"""

from __future__ import annotations

from typing import Any

import numpy as np

# ----------------------------------------------------------------------
# Examine (returns DataFrames / GeoDataFrame via your query())
# ----------------------------------------------------------------------


def coverage(engine: Any, table: str, schema: str = "raw_data"):
    """Tile count and the overall stored extent (uses the extent columns)."""
    return engine.query(
        f"""
        select count(*) as n_tiles,
               min(min_x) as min_x, min(min_y) as min_y,
               max(max_x) as max_x, max(max_y) as max_y
        from {schema}.{table}
        where "valid_to" is null
        """
    )


def value_stats(engine: Any, table: str, schema: str = "raw_data"):
    """
    Elevation min/max/mean across the whole table. This is the first
    sanity check: a sane metro range (and no -9999 / -999999 leaking in
    as the min) tells you nodata is being honored.
    """
    return engine.query(
        f"""
        select count(*)        as n_tiles,
               sum((ss).count) as n_pixels,
               min((ss).min)   as min_elev_m,
               max((ss).max)   as max_elev_m,
               avg((ss).mean)  as approx_mean_m
        from (
            select ST_SummaryStats(rast) as ss
            from {schema}.{table}
            where "valid_to" is null
        ) t
        """
    )


def sample_point(
    engine: Any, table: str, lon: float, lat: float, schema: str = "raw_data", srid: int = 4269
):
    """Elevation at a coordinate — index-assisted by ST_Intersects."""
    return engine.query(
        f"""
        select ST_Value(rast, ST_SetSRID(ST_Point(%(lon)s, %(lat)s), {srid}),
                        resample => 'bilinear') as elevation_m
        from {schema}.{table}
        where "valid_to" is null
          and ST_Intersects(rast, ST_SetSRID(ST_Point(%(lon)s, %(lat)s), {srid}))
        """,
        {"lon": lon, "lat": lat},
    )


def footprints(engine: Any, table: str, schema: str = "raw_data"):
    """
    Tile footprints as geometry. Because the column is geometry, query()
    returns a GeoDataFrame — so `footprints(...).plot()` draws the tile
    grid and instantly shows whether the clip worked and there are no gaps.
    """
    return engine.query(
        f"""
        select tile_id, ST_Envelope(rast) as geom
        from {schema}.{table}
        where "valid_to" is null
        """
    )


# ----------------------------------------------------------------------
# Fetch pixels into a numpy array (for rendering)
# ----------------------------------------------------------------------


def fetch_array(
    engine: Any,
    table: str,
    schema: str = "raw_data",
    srid: int = 4269,
    bbox: tuple[float, float, float, float] | None = None,
):
    """
    Pull the current sub-tiles and mosaic them into one masked numpy
    array client-side — no server-side ST_Union, and no GDAL drivers.

    Why not ST_Union: it requires every raster share an exact pixel
    alignment (same scale/skew, origins an integer number of pixels
    apart). 3DEP publishes each 1-degree tile independently, and their
    geotransforms aren't guaranteed bit-identical, so a region spanning
    more than one source tile trips ST_Union with
    "rt_raster_from_two_rasters: ... do not have the same alignment".
    Mosaicking here snaps each sub-tile to a common grid (nearest cell),
    which sidesteps that. (Also avoids ST_AsTIFF, which needs PostGIS's
    GDAL output drivers — disabled by default.)

    Returns (array, extent) with extent=(west, east, south, north) for
    matplotlib imshow. nodata pixels come back as NULL and are masked.

    Pass a small bbox=(w, s, e, n) while iterating so the pull stays cheap.
    """
    if bbox is not None:
        w, s, e, n = bbox
        rast = "ST_Clip(rast, env.g)"  # crops to the bbox, preserves grid
        frm = (
            f"from {schema}.{table}, "
            f"lateral (select ST_MakeEnvelope({w}, {s}, {e}, {n}, {srid}) as g) env "
            f'where "valid_to" is null and ST_Intersects(rast, env.g)'
        )
    else:
        rast = "rast"
        frm = f'from {schema}.{table} where "valid_to" is null'

    rows = engine.query(
        f"""
        select ST_DumpValues({rast}, 1) as vals,
               ST_UpperLeftX({rast}) as ulx,
               ST_UpperLeftY({rast}) as uly,
               ST_ScaleX({rast})     as sx,
               ST_ScaleY({rast})     as sy
        {frm}
        """,
        as_dicts=True,
    )

    tiles = [
        (_to_masked_array(r["vals"]), float(r["ulx"]), float(r["uly"]))
        for r in rows
        if r["vals"] is not None
    ]
    if not tiles:
        raise ValueError("no raster returned (empty table or bbox outside coverage)")

    return _mosaic(tiles, sx=float(rows[0]["sx"]), sy=float(rows[0]["sy"]))


def _mosaic(tiles: list, sx: float, sy: float):
    """
    Place each (masked array, upper-left x, upper-left y) onto one canvas
    on a shared grid. sx > 0, sy < 0 (north-up). Sub-tiles are snapped to
    the nearest cell, so minor cross-tile geotransform drift is absorbed;
    overlapping collars don't overwrite good data with nodata.
    """
    ph = -sy  # pixel height, positive
    global_ulx = min(ulx for _, ulx, _ in tiles)
    global_uly = max(uly for _, _, uly in tiles)

    placed, nrows, ncols = [], 0, 0
    for arr, ulx, uly in tiles:
        col0 = int(round((ulx - global_ulx) / sx))
        row0 = int(round((global_uly - uly) / ph))
        h, w = arr.shape
        placed.append((arr, row0, col0, h, w))
        nrows, ncols = max(nrows, row0 + h), max(ncols, col0 + w)

    master = np.full((nrows, ncols), np.nan, dtype="float64")
    for arr, row0, col0, h, w in placed:
        filled = arr.filled(np.nan)
        have = ~np.isnan(filled)
        master[row0 : row0 + h, col0 : col0 + w][have] = filled[have]

    arr = np.ma.masked_invalid(master)
    extent = (global_ulx, global_ulx + ncols * sx, global_uly + nrows * sy, global_uly)
    return arr, extent


def _to_masked_array(vals: list) -> np.ma.MaskedArray:
    """
    Turn ST_DumpValues output (a nested list, with None for nodata/NULL
    pixels — psycopg2's rendering of a Postgres double precision[][]) into
    a masked float array.
    """
    a = np.array(
        [[np.nan if v is None else float(v) for v in row] for row in vals],
        dtype="float64",
    )
    return np.ma.masked_invalid(a)


# ----------------------------------------------------------------------
# Render
# ----------------------------------------------------------------------


def plot_elevation(
    arr, extent=None, *, title="Elevation (m)", hillshade=True, cmap="terrain", ax=None
):
    """
    Render an elevation array. Pass the (array, extent) from fetch_array.
    Draws a colored elevation map, optionally blended with a hillshade
    for relief (hillshade is invisible on truly flat terrain, which is
    itself informative — e.g. Chicago vs Denver).
    """
    import matplotlib.pyplot as plt
    from matplotlib.colors import LightSource

    masked = np.ma.masked_invalid(arr)

    if ax is None:
        _, ax = plt.subplots(figsize=(7, 6))

    if hillshade and masked.count() > 0:
        ls = LightSource(azdeg=315, altdeg=45)
        filled = masked.filled(np.ma.median(masked))
        shade = ls.hillshade(filled, vert_exag=10)
        ax.imshow(shade, extent=extent, cmap="gray", alpha=1.0, origin="upper", aspect="auto")
        im = ax.imshow(masked, extent=extent, cmap=cmap, alpha=0.6, origin="upper", aspect="auto")
    else:
        im = ax.imshow(masked, extent=extent, cmap=cmap, origin="upper", aspect="auto")

    cbar = ax.figure.colorbar(im, ax=ax, shrink=0.8)
    cbar.set_label("meters")
    ax.set_title(title)
    if extent is not None:
        ax.set_xlabel("lon")
        ax.set_ylabel("lat")
    return ax
