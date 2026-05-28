{#
    Per-segment intrinsic stress costs derived from the physical attributes of
    the segment (highway class, infrastructure, surface, etc).
    Only OSM data required.

    Source: stg_<city>_bike_segments
    Grain: one row per segment (segment_id).

    Cost model:
      physical_cost = length_m * (base_stress_per_meter
                                  + surface_penalty
                                  + tunnel_penalty
                                  + lighting_penalty)

    The base lookup is keyed on (highway class, infra type) and reads as
    "stress-equivalent meters per meter of segment": a value of 6.0 means
    100m of this segment feels like 600m of calm riding. The minimum is
    1.0 (cycleway / path), so stress_cost >= length_m always.

    The three penalties are additive on top of the base:
      - surface_penalty: +0.3 on rough surfaces (cobblestone, gravel, dirt)
      - tunnel_penalty:  +0.5 on car-tunnels without separated bike infra
      - lighting_penalty: +0.1 on unlit segments

    Parameters:
      city: city name; used to resolve stg_<city>_bike_segments.
#}
{% macro generate_city_segment_costs_model(city) %}

with classified as (
    select
        s.*,

        -- Bucket the OSM highway value into a stress class. The class
        -- drives the base stress lookup below. Motorway is included for
        -- completeness but shouldn't appear in the bike network ref
        -- since we filter access=no earlier in the pipeline.
        case
            when s.highway in ('cycleway', 'path', 'footway',
                               'pedestrian', 'bridleway', 'steps')   then 'quiet'
            when s.highway in ('service')                              then 'service'
            when s.highway in ('residential', 'unclassified',
                               'living_street', 'busway')              then 'local'
            when s.highway in ('tertiary', 'tertiary_link')            then 'tertiary'
            when s.highway in ('secondary', 'secondary_link')          then 'secondary'
            when s.highway in ('primary', 'primary_link')              then 'primary'
            when s.highway in ('motorway', 'motorway_link', 'trunk',
                               'trunk_link')                            then 'motorway'
            else 'local'
        end as highway_class,

        -- Bucket infra_type into broader quality tiers. NULL infra is
        -- "no bike-specific infrastructure on this segment".
        case
            when s.infra_type in ('protected_lane', 'track')           then 'protected'
            when s.infra_type in ('buffered_lane', 'bike_lane',
                                  'bicycle_road')                       then 'lane'
            when s.infra_type in ('shared_path', 'designated_path')    then 'shared_path'
            when s.infra_type in ('sharrow', 'share_busway')           then 'sharrow'
            else 'none'
        end as infra_tier

    from {{ ref('stg_' ~ city ~ '_bike_segments') }} s
),

with_base as (
    select
        *,

        -- Base stress per meter, by (highway_class, infra_tier).
        -- See issue #N for derivation. Tuned so the minimum is 1.0
        -- (cycleway / path) and the worst case is ~6.0 (primary with
        -- no bike infra). Service ways sit at 4.0 to discourage
        -- routing through alleys and parking lots except as
        -- last-resort connectors.
        case
            when highway_class = 'quiet'                               then 1.0

            when highway_class = 'local' and infra_tier = 'protected'  then 1.1
            when highway_class = 'local' and infra_tier = 'lane'       then 1.3
            when highway_class = 'local'                               then 1.8

            when highway_class = 'tertiary' and infra_tier = 'protected' then 1.2
            when highway_class = 'tertiary' and infra_tier = 'lane'    then 1.8
            when highway_class = 'tertiary'                            then 2.8

            when highway_class = 'secondary' and infra_tier = 'protected' then 1.3
            when highway_class = 'secondary' and infra_tier = 'lane'   then 2.5
            when highway_class = 'secondary'                           then 4.5

            when highway_class = 'primary' and infra_tier = 'protected' then 1.5
            when highway_class = 'primary' and infra_tier = 'lane'     then 3.5
            when highway_class = 'primary'                             then 6.0

            -- Service: alleys, parking aisles, driveways. Last-resort
            -- connectors; we want the router to avoid these unless
            -- the alternative is much worse.
            when highway_class = 'service' and infra_tier in ('protected', 'lane') then 3.0
            when highway_class = 'service'                             then 4.0

            -- Motorway shouldn't appear post-access-filtering, but
            -- give it a high cost so any leakage routes around it.
            when highway_class = 'motorway'                            then 10.0

            -- Fallback for anything unclassified.
            else 2.0
        end as base_stress_per_meter,

        -- Additive surface penalty. Applied uniformly across all
        -- segments — a cobblestone cycleway is still uncomfortable
        -- regardless of car traffic. NULL surface is treated as
        -- paved (good OSM coverage in current target cities; revisit
        -- for rural extracts).
        case
            when surface in ('cobblestone', 'unhewn_cobblestone',
                             'sett', 'gravel', 'fine_gravel',
                             'unpaved', 'dirt', 'ground',
                             'mud', 'sand')                            then 0.3
            else 0.0
        end as surface_penalty,

        -- Tunnel penalty. Only applies to car-tunnels where the
        -- cyclist shares space with cars; separated infra in a
        -- tunnel doesn't get penalized. `tunnel=culvert` and other
        -- non-cyclist values are excluded.
        case
            when tunnel in ('yes', 'building_passage')
                and highway_class in ('local', 'tertiary',
                                      'secondary', 'primary', 'service')
                and infra_tier not in ('protected', 'shared_path')     then 0.5
            else 0.0
        end as tunnel_penalty,

        -- Lighting penalty. Small additive cost for unlit segments.
        case
            when lit = 'no'                                            then 0.1
            else 0.0
        end as lighting_penalty

    from classified
)

select
    -- Identity
    segment_id,
    way_id,
    start_position,
    end_position,

    -- Topology
    start_node_id,
    end_node_id,
    direction,

    -- Geometry and length
    geom,
    length_m,

    -- Identification
    name,
    highway,

    -- Bike infrastructure
    infra_category,
    infra_type,
    has_buffer,

    -- Physical conditions
    surface,
    lit,
    bridge,
    tunnel,
    maxspeed,

    -- Raw cycleway tags
    cycleway,
    cycleway_left,
    cycleway_right,

    -- Cost classification (preserved for tuning visibility)
    highway_class,
    infra_tier,
    base_stress_per_meter,
    surface_penalty,
    tunnel_penalty,
    lighting_penalty,

    -- Final per-segment physical cost. Always >= length_m since
    -- base_stress_per_meter >= 1.0 and the penalties are >= 0.
    length_m * (
        base_stress_per_meter
        + surface_penalty
        + tunnel_penalty
        + lighting_penalty
    ) as physical_cost

from with_base

{% endmacro %}
