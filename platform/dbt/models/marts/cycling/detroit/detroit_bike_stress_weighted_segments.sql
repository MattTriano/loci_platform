{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{ this }} (segment_id)",
        "CREATE INDEX ON {{ this }} (start_node_id)",
        "CREATE INDEX ON {{ this }} (end_node_id)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "RESET work_mem"
    ]
) }}

with segment_costs as (
    select * from {{ ref('detroit_segment_costs') }}
),
intersection_costs as (
    select * from {{ ref('detroit_intersection_costs') }}
)

select
    -- Identity
    sc.segment_id,
    sc.way_id,
    sc.start_position,
    sc.end_position,

    -- Topology
    sc.start_node_id,
    sc.end_node_id,
    sc.direction,

    -- Geometry and length
    sc.geom,
    sc.length_m,

    -- Identification
    sc.name,
    sc.highway,

    -- Bike infrastructure
    sc.infra_category,
    sc.infra_type,
    sc.has_buffer,

    -- Physical conditions
    sc.surface,
    sc.lit,
    sc.bridge,
    sc.tunnel,
    sc.maxspeed,

    -- Raw cycleway tags
    sc.cycleway,
    sc.cycleway_left,
    sc.cycleway_right,

    -- Physical factors (preserved for tuning)
    sc.speed_factor,
    sc.road_type_factor,
    sc.infrastructure_factor,
    sc.tunnel_factor,
    sc.surface_factor,
    sc.lighting_factor,

    -- Cost components
    sc.physical_cost,
    coalesce(ic.intersection_cost, 0) as intersection_cost,

    -- Final composition
    sc.physical_cost
        + coalesce(ic.intersection_cost, 0)
        as stress_cost

from segment_costs as sc
left join intersection_costs as ic
    on ic.osmid = sc.end_node_id
