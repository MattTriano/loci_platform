{% macro generate_stg_city_way_segments_model(city) %}

with way_cuts as (
    select
        wn.way_id,
        wn.position,
        wn.node_id
    from {{ ref('stg_' ~ city ~ '_way_nodes') }} wn
    inner join {{ ref('stg_' ~ city ~'_intersection_nodes') }} i
        on wn.node_id = i.node_id
),
segments as (
    select
        way_id,
        node_id as start_node_id,
        position as start_position,
        lead(node_id) over (partition by way_id order by position) as end_node_id,
        lead(position) over (partition by way_id order by position) as end_position
    from way_cuts
),
way_points as (
    -- Dump every vertex of every way with its position. Use this to
    -- reconstruct segment geometries vertex-by-vertex rather than via
    -- length-fraction interpolation.
    select
        osm_id as way_id,
        (dp).path[1] as position,
        (dp).geom as point_geom
    from (
        select osm_id, ST_DumpPoints(geom) as dp
        from {{ ref('stg_' ~ city ~ '_bike_ways') }}
    ) t
),
segment_geoms as (
    select
        s.way_id,
        s.start_node_id,
        s.end_node_id,
        s.start_position,
        s.end_position,
        ST_MakeLine(wp.point_geom order by wp.position) as geom
    from segments s
    inner join way_points wp
        on wp.way_id = s.way_id
       and wp.position between s.start_position and s.end_position
    where s.end_node_id is not null
    group by s.way_id, s.start_node_id, s.end_node_id, s.start_position, s.end_position
)
select
    sg.way_id,
    sg.start_node_id,
    sg.end_node_id,
    sg.start_position,
    sg.end_position,
    sg.geom,
    w.highway,
    w.name,
    w.bicycle,
    w.cycleway,
    w.cycleway_left,
    w.cycleway_right,
    w.surface,
    w.lit,
    w.bridge,
    w.tunnel,
    w.cycleway_buffer,
    w.cycleway_left_buffer,
    w.cycleway_right_buffer,
    w.cycleway_both_buffer,
    w.cycleway_separation,
    w.cycleway_left_separation,
    w.cycleway_right_separation,
    w.cycleway_both_separation,
    w.cycleway_shared_lane,
    w.cycleway_left_shared_lane,
    w.cycleway_right_shared_lane,
    w.cycleway_both_shared_lane,
    w.cycleway_both,
    w.class_bicycle,
    w.bicycle_road,
    w.cyclestreet,
    w.maxspeed,
    w.direction,
    w.oneway,
    w.tags
from segment_geoms sg
inner join {{ ref('stg_' ~ city ~ '_bike_ways') }} w on sg.way_id = w.osm_id

{% endmacro %}