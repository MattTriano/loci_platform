{#
    One row per routing-graph node with its bare-earth elevation, sampled from
    the region's USGS 3DEP DEM (raw_data.<city>_3dep_elevation).

    Node geometry comes from the segment endpoints: segment geom is ordered
    start -> end, so the start node is ST_StartPoint and the end node is
    ST_EndPoint. OSM geometries are SRID 4326; the raster is NAD83 / 4269
    (NAVD88 meters), so points are transformed to 4269 before sampling.

    Tile resolution goes through stg_<city>_elevation_tiles (precomputed,
    GiST-indexed hulls) via a lateral limit-1 probe, so each node resolves to
    exactly one tile before any sampling and ST_Value is evaluated once per
    node. The matching raster is then fetched by tile_id (uses the source's
    current-rows btree) and the pixel read with bilinear resampling.

    elevation_m is left NULL where the point falls outside raster coverage or
    on a nodata pixel (e.g. open water): a node with no intersecting hull gets
    a NULL tile_id, the raster join finds nothing, and ST_Value returns NULL.
    The cost model treats NULL as flat; geom is kept so NULL nodes can be
    inspected on a map.

    Source: stg_<city>_bike_segments, stg_<city>_elevation_tiles,
            source('threedep', '<city>_3dep_elevation')
    Grain: one row per node_id.
#}
{% macro generate_stg_city_node_elevations_model(city) %}

with node_points as (
    select start_node_id as node_id, ST_StartPoint(geom) as geom
    from {{ ref('stg_' ~ city ~ '_bike_segments') }}
    union all
    select end_node_id as node_id, ST_EndPoint(geom) as geom
    from {{ ref('stg_' ~ city ~ '_bike_segments') }}
),
distinct_nodes as (
    -- One point per node. Adjacent segments share the exact OSM vertex, so a
    -- node_id always resolves to a single geometry.
    select distinct on (node_id) node_id, geom
    from node_points
    order by node_id
),
points_4269 as (
    select node_id, geom, ST_Transform(geom, 4269) as geom_4269
    from distinct_nodes
),
node_tiles as (
    -- Resolve each node to exactly one tile via an indexed hull probe.
    -- The && uses the GiST index on stg_<city>_elevation_tiles.hull; the
    -- distance order + limit 1 picks a single tile and kills the boundary
    -- fan-out that would otherwise multiply ST_Value calls. Nodes outside
    -- all hulls get a NULL tile_id (left join lateral preserves the row).
    select p.node_id, p.geom, p.geom_4269, t.tile_id
    from points_4269 p
    left join lateral (
        select th.tile_id
        from {{ ref('stg_' ~ city ~ '_elevation_tiles') }} th
        where th.hull && p.geom_4269
        order by th.hull <-> p.geom_4269
        limit 1
    ) t on true
),
sampled as (
    -- Fetch the one matching raster by tile_id and read the pixel. Band 1
    -- (single-band DEM); the band arg is required to reach the ST_Value
    -- overload that accepts resample.
    select
        nt.node_id,
        nt.geom,
        ST_Value(r.rast, 1, nt.geom_4269, resample => 'bilinear') as elevation_m
    from node_tiles nt
    left join {{ source('threedep', city ~ '_3dep_elevation') }} r
        on r.valid_to is null
       and r.tile_id = nt.tile_id
)
select
    node_id,
    geom,
    elevation_m
from sampled

{% endmacro %}
