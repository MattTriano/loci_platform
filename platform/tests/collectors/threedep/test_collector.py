# /loci_platform/platform/tests/collectors/threedep/test_collector.py
"""
Critical observable behaviors of ThreeDEPCollector, tested end to end
against a real Postgres (real StagedIngest SCD2 semantics and the raster
COPY/ST_Value path) with a fake source at the HTTP boundary.

Each test names one behavior the tooling must keep exhibiting. The
assertions look only at the target table and the collect() summary, so
the implementation underneath is free to change.
"""

from __future__ import annotations

from .helpers import (
    SINGLE_TILE_BBOX,
    TWO_TILE_BBOX,
    make_elevation_spec,
    seeded_source,
)

TABLE = "fake_elevation"


def total_count(engine, schema) -> int:
    df = engine.query(f"select count(*) as n from {schema}.{TABLE}")
    return int(df["n"].iloc[0])


def current_count(engine, schema) -> int:
    df = engine.query(f'select count(*) as n from {schema}.{TABLE} where "valid_to" is null')
    return int(df["n"].iloc[0])


def sample_elev(engine, schema, lon, lat, srid=4269):
    df = engine.query(
        f"""
        select ST_Value(rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), {srid})) as elev
        from {schema}.{TABLE}
        where "valid_to" is null
          and ST_Intersects(rast, ST_SetSRID(ST_Point(%(x)s, %(y)s), {srid}))
        """,
        {"x": lon, "y": lat},
    )
    return None if df.empty else df.iloc[0]["elev"]


# ---------------------------------------------------------------------
# Behavior 1: a first collect ingests the tiles covering the bbox,
# clips the stored sub-tiles to it, and the result samples a finite
# elevation at a point inside the region.
# ---------------------------------------------------------------------


def test_first_collect_ingests_clips_and_samples(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=SINGLE_TILE_BBOX)
    collector = warehouse(spec, seeded_source(["n42w088"]))

    summary = collector.collect(spec, force=True)

    assert summary["errors"] == []
    assert summary["tiles_collected"] == 1
    assert summary["rows_merged"] > 0

    # Clip: every stored sub-tile intersects the bbox.
    rows = engine.query(
        f'select min_x, min_y, max_x, max_y from {schema}.{TABLE} where "valid_to" is null',
        as_dicts=True,
    )
    assert len(rows) > 0
    b = SINGLE_TILE_BBOX
    for r in rows:
        assert not (
            r["max_x"] < b.west
            or r["min_x"] > b.east
            or r["max_y"] < b.south
            or r["min_y"] > b.north
        )

    cx, cy = (b.west + b.east) / 2, (b.south + b.north) / 2
    assert sample_elev(engine, schema, cx, cy) is not None


# ---------------------------------------------------------------------
# Behavior 2: an incremental re-run skips tiles already present, without
# re-downloading them.
# ---------------------------------------------------------------------


def test_incremental_skips_present_without_download(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=SINGLE_TILE_BBOX)
    collector = warehouse(spec, seeded_source(["n42w088"]))
    collector.collect(spec, force=True)

    collector.client.downloaded.clear()
    summary = collector.collect(spec)  # incremental

    assert summary["mode"] == "incremental"
    assert summary["tiles_skipped_present"] == 1
    assert summary["tiles_collected"] == 0
    assert collector.client.downloaded == []


# ---------------------------------------------------------------------
# Behavior 3: recollection (force) never duplicates data.
# ---------------------------------------------------------------------


def test_force_recollect_creates_no_duplicates(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=SINGLE_TILE_BBOX)
    collector = warehouse(spec, seeded_source(["n42w088"]))
    collector.collect(spec, force=True)
    n = total_count(engine, schema)

    summary = collector.collect(spec, force=True)

    assert summary["rows_merged"] == 0
    assert total_count(engine, schema) == n


# ---------------------------------------------------------------------
# Behavior 4: a re-staged tile (changed pixels) versions the changed
# sub-tiles on a forced run, then resettles. (Incremental skips present
# tiles, so force is how a re-stage is picked up — the documented
# semantics.)
# ---------------------------------------------------------------------


def test_restaged_tile_versions_then_resettles(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=SINGLE_TILE_BBOX)
    source = seeded_source(["n42w088"])
    collector = warehouse(spec, source)
    collector.collect(spec, force=True)

    current_before = current_count(engine, schema)
    total_before = total_count(engine, schema)

    source.set_tile("n42w088", base_value=5000.0)  # USGS re-stages with new values
    summary = collector.collect(spec, force=True)

    assert summary["rows_merged"] == current_before  # every sub-tile changed
    assert current_count(engine, schema) == current_before  # one current per sub-tile
    assert total_count(engine, schema) == total_before + current_before  # old versions kept

    assert collector.collect(spec, force=True)["rows_merged"] == 0  # resettled


# ---------------------------------------------------------------------
# Behavior 5: a tile not staged at the source is skipped, not fatal.
# ---------------------------------------------------------------------


def test_missing_at_source_tile_is_skipped_not_fatal(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=TWO_TILE_BBOX)  # needs n42w088 + n43w088
    collector = warehouse(spec, seeded_source(["n42w088"]))  # only one staged

    summary = collector.collect(spec, force=True)

    assert summary["tiles_collected"] == 1
    assert summary["tiles_missing_at_source"] == 1
    assert summary["errors"] == []
    assert total_count(engine, schema) > 0


# ---------------------------------------------------------------------
# Behavior 6: a failing tile doesn't block the others; the failure is
# reported in the summary.
# ---------------------------------------------------------------------


def test_failure_in_one_tile_does_not_block_others(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=TWO_TILE_BBOX)
    collector = warehouse(spec, seeded_source(["n42w088", "n43w088"]))
    collector.client.fail_tiles.add("n42w088")

    summary = collector.collect(spec, force=True)

    assert summary["tiles_collected"] == 1
    assert len(summary["errors"]) == 1
    assert summary["errors"][0]["tile"] == "n42w088"
    assert total_count(engine, schema) > 0  # n43w088 still landed


# ---------------------------------------------------------------------
# Behavior 7: collecting into a missing target table fails per-tile with
# reported errors, not an unhandled crash.
# ---------------------------------------------------------------------


def test_collect_without_target_table_reports_errors_not_crash(engine, schema, warehouse):
    spec = make_elevation_spec(schema, bbox=TWO_TILE_BBOX)
    collector = warehouse(spec, seeded_source(["n42w088", "n43w088"]), create_table=False)

    summary = collector.collect(spec, force=True)

    assert summary["tiles_collected"] == 0
    assert len(summary["errors"]) == 2
