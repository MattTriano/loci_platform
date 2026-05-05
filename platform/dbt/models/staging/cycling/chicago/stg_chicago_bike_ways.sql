-- models/staging/cycling/chicago/stg_chicago_bike_ways.sql
{{ config(materialized='view') }}

select
    osm_id,
    osm_version,
    osm_timestamp,
    geom,
    node_ids,
    array_length(node_ids, 1) as node_count,

    -- Promoted columns (from the spec)
    highway,
    name,
    bicycle,
    cycleway,
    cycleway_left,
    cycleway_right,
    surface,
    maxspeed,

    -- Physical conditions for stress weighting
    tags->>'lit' as lit,
    tags->>'bridge' as bridge,
    tags->>'tunnel' as tunnel,

    -- Cycleway sub-tags for infra classification
    tags->>'cycleway:buffer'             as cycleway_buffer,
    tags->>'cycleway:left:buffer'        as cycleway_left_buffer,
    tags->>'cycleway:right:buffer'       as cycleway_right_buffer,
    tags->>'cycleway:both:buffer'        as cycleway_both_buffer,
    tags->>'cycleway:separation'         as cycleway_separation,
    tags->>'cycleway:left:separation'    as cycleway_left_separation,
    tags->>'cycleway:right:separation'   as cycleway_right_separation,
    tags->>'cycleway:both:separation'    as cycleway_both_separation,
    tags->>'cycleway:shared_lane'        as cycleway_shared_lane,
    tags->>'cycleway:left:shared_lane'   as cycleway_left_shared_lane,
    tags->>'cycleway:right:shared_lane'  as cycleway_right_shared_lane,
    tags->>'cycleway:both:shared_lane'   as cycleway_both_shared_lane,
    tags->>'cycleway:both'               as cycleway_both,
    tags->>'class:bicycle'               as class_bicycle,
    tags->>'bicycle_road'                as bicycle_road,
    tags->>'cyclestreet'                 as cyclestreet,
    tags->>'service'                     as service,
    tags->>'access'                      as access,

    -- Canonical bike-routing direction
    case
        when coalesce(tags->>'oneway:bicycle', oneway) in ('yes', 'true', '1') then 'forward'
        when coalesce(tags->>'oneway:bicycle', oneway) = '-1' then 'backward'
        when coalesce(tags->>'oneway:bicycle', oneway) in ('no', 'false', '0') then 'bidirectional'
        when coalesce(tags->>'oneway:bicycle', oneway) is null then 'bidirectional'
        else 'bidirectional'
    end as direction,
    oneway,

    tags,
    ingested_at

from {{ source('osm', 'chicago_osm_bike_network_edges') }}
where
    valid_to is null
    and osm_type = 'way'
    and geometrytype(geom) = 'LINESTRING'
    and (
        highway != 'footway'
        or bicycle in ('yes', 'designated', 'permissive')
    ) and (
        (tags->>'access' is null)
        or (tags->>'access' not in ('private', 'no', 'customers', 'permit'))
    )
