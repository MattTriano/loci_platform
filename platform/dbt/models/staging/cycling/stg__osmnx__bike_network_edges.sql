-- stg__osmnx__bike_network_edges.sql
-- Staging model for OSMnx bike network edges over the Chicagoland bbox.
--
-- Source: raw_data.osmnx_bike_network_edges (collected via OsmnxCollector)
-- Grain: one row per directed edge (u, v, key), current version only.
--
-- Notes:
--   - osmid may be a stringified list (e.g. "[123, 456]") for edges that
--     were simplified from multiple OSM ways. Treat as provenance only.
--   - reversed is an OSMnx-derived flag (not an OSM tag) indicating the
--     edge direction was synthesized by reversing an undirected OSM way.
--     Treat as provenance; useful for debugging unexpected routing.
--   - tile_id is ingestion metadata and is excluded from this model.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

select
    -- Graph topology
    md5(u || '|' || v || '|' || key) as segment_id,
    u,
    v,
    key,
    osmid,

    -- Road attributes
    name,
    highway,
    oneway,
    reversed,
    lanes,
    ref,
    service,
    width,
    maxspeed,
    access,

    -- Bicycle general
    bicycle,
    bicycle_lanes,
    bicycle_lanes_backward,
    bicycle_lanes_forward,
    bicycle_right,
    bicycle_road,
    class_bicycle,
    cyclestreet,
    oneway_bicycle,
    ramp_bicycle,
    sidewalk_both_bicycle,

    -- Cycleway general
    cycleway,
    cycleway_buffer,
    cycleway_lane,
    cycleway_oneway,
    cycleway_separation,
    cycleway_shared_lane,
    cycleway_smoothness,
    cycleway_surface,

    -- Cycleway: both sides
    cycleway_both,
    cycleway_both_buffer,
    cycleway_both_colour,
    cycleway_both_lane,
    cycleway_both_separation,
    cycleway_both_shared_lane,
    cycleway_both_traffic_sign,

    -- Cycleway: left
    cycleway_left,
    cycleway_left_buffer,
    cycleway_left_lane,
    cycleway_left_oneway,
    cycleway_left_separation,
    cycleway_left_shared_lane,
    cycleway_left_traffic_sign,

    -- Cycleway: right
    cycleway_right,
    cycleway_right_buffer,
    cycleway_right_lane,
    cycleway_right_oneway,
    cycleway_right_separation,
    cycleway_right_shared_lane,
    cycleway_right_traffic_sign,

    -- Physical conditions
    surface,
    lit,
    bridge,
    tunnel,

    -- Geometry and distance
    length_m,
    geom

from {{ source('raw_data', 'osmnx_bike_network_edges') }}
where
    valid_to is null
    and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
    and u is not null
    and v is not null
    and key is not null
