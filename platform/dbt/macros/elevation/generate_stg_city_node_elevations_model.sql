{#
    One row per routing-graph node with its bare-earth elevation, sampled from
    the region's USGS 3DEP DEM (raw_data.<city>_3dep_elevation).

    Node geometry comes from the segment endpoints: segment geom is ordered
    start -> end, so the start node is ST_StartPoint and the end node is
    ST_EndPoint. OSM geometries are SRID 4326; the raster is NAD83 / 4269
    (NAVD88 meters), so points are transformed to 4269 before sampling.

    elevation_m is left NULL where the point falls outside raster coverage or
    on a nodata pixel (e.g. open water). The cost model treats NULL as flat; the
    geom column is kept so NULL nodes can be inspected on a map.

    Source: stg_<city>_bike_segments, source('threedep', '<city>_3dep_elevation')
    Grain: one row per node_id.
#}
{% macro generate_stg_city_node_elevations_model(city) %}

with tile_hulls as materialized (
    -- Compute each current tile's footprint ONCE (60 rows, 60 detoasts total).
    -- Without `materialized` the planner inlines this and recomputes the hull
    -- per point, which is the brute-force detoast that made this slow.
    select tile_id, ST_ConvexHull(rast) as hull
    from {{ source('threedep', city ~ '_3dep_elevation') }}
    where valid_to is null
),
node_points as (
    select start_node_id as node_id, ST_StartPoint(geom) as geom
    from {{ ref('stg_' ~ city ~ '_bike_segments') }}
    union all
    select end_node_id as node_id, ST_EndPoint(geom) as geom
    from {{ ref('stg_' ~ city ~ '_bike_segments') }}
),
distinct_nodes as (
    select distinct on (node_id) node_id, geom
    from node_points
    order by node_id
),
points_4269 as (
    select node_id, geom, ST_Transform(geom, 4269) as geom_4269
    from distinct_nodes
),
node_tiles as (
    -- Resolve each node to its tile against the cheap precomputed hulls.
    select p.node_id, p.geom, p.geom_4269, h.tile_id
    from points_4269 p
    left join tile_hulls h
        on ST_Intersects(h.hull, p.geom_4269)
),
sampled as (
    -- Now fetch the one matching raster by tile_id (uses the existing
    -- ix_<city>_3dep_elevation_current btree) and read the pixel.
    select
        nt.node_id,
        nt.geom,
        ST_Value(r.rast, 1, nt.geom_4269, resample => 'bilinear') as elevation_m
    from node_tiles nt
    left join {{ source('threedep', city ~ '_3dep_elevation') }} r
        on r.valid_to is null
       and r.tile_id = nt.tile_id
)
select distinct on (node_id)
    node_id, geom, elevation_m
from sampled
order by node_id, (elevation_m is null)

{% endmacro %}
