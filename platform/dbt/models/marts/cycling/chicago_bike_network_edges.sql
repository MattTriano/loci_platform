-- chicago_bike_network_edges.sql
-- OSMnx bike network edges enriched with OSM-derived infrastructure
-- classification.
--
-- Grain: one row per directed OSMnx edge (u, v, key).
-- Edges with no bike infrastructure get null for infra columns.
--
-- Infrastructure classification is provided by the
-- classify_bike_infrastructure macro. See that macro for the full
-- taxonomy and tag resolution logic.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

with edges as (
    select * from {{ ref('stg__osmnx_chicago_bike_network_edges') }}
),

classified as (
    {{ classify_bike_infrastructure('edges') }}
),

final as (
    select
        -- Graph topology
        u,
        v,
        key,
        segment_id,
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
        highway_raw,

        -- Bike infrastructure: OSM-derived classification
        osm_infra_category,
        osm_infra_type,
        __osm_has_buffer as osm_has_buffer,

        -- Bike infrastructure: OSM raw tags (for downstream refinement)
        cycleway,
        cycleway_right,
        cycleway_left,
        cycleway_both,
        cycleway_separation,
        cycleway_right_separation,
        cycleway_left_separation,
        cycleway_both_separation,
        bicycle,
        class_bicycle,
        bicycle_road,
        cyclestreet,

        -- Physical conditions
        surface,
        cycleway_surface,
        cycleway_smoothness,
        lit,
        bridge,
        tunnel,

        -- Geometry and distance
        length_m,
        geom

    from classified
)

select * from final
