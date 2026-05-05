-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_bike_segments.sql
-- Bike network segments, one row per intersection-to-intersection
-- portion of a way, with bike infrastructure classification and
-- physical conditions joined in.
--
-- Source: stg_chicago_way_segments + stg_chicago_osm_bike_infra
-- Grain: one row per segment (way_id, start_position, end_position).
--
-- Direction handling: each segment carries a `direction` column
-- ('forward', 'backward', 'bidirectional') indicating which directions
-- of travel are legal. The graph exporter expands these into one or
-- two directed edges per segment.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} (way_id, start_position, end_position)",
        "CREATE INDEX ON {{ this }} (start_node_id)",
        "CREATE INDEX ON {{ this }} (end_node_id)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

select
    -- Identity
    {{ dbt_utils.generate_surrogate_key(['s.way_id', 's.start_position', 's.end_position']) }}
        as segment_id,
    s.way_id,
    s.start_position,
    s.end_position,

    -- Topology
    s.start_node_id,
    s.end_node_id,
    s.direction,

    -- Geometry
    s.geom,
    ST_Length(s.geom::geography) as length_m,

    -- Identification
    s.name,
    s.highway,

    -- Bike infrastructure (NULL when no bike infra on this segment)
    i.infra_category,
    i.infra_type,
    i.has_buffer,

    -- Stress factor inputs
    s.surface,
    s.lit,
    s.bridge,
    s.tunnel,
    s.maxspeed,
    s.bicycle,

    -- Raw cycleway tags preserved for downstream refinement
    s.cycleway,
    s.cycleway_left,
    s.cycleway_right,

    s.tags

from {{ ref('stg_chicago_way_segments') }} s
left join {{ ref('stg_chicago_osm_bike_infra') }} i
    on i.way_id = s.way_id
   and i.start_position = s.start_position
   and i.end_position = s.end_position
