-- chicago_bike_stress_weighted_segments.sql
-- Assigns a stress cost to each bike network segment for use in
-- stress-weighted routing.
--
-- Cost formula:
--   stress_cost = length_m
--               * speed_factor
--               * road_type_factor
--               * infrastructure_factor
--               * tunnel_factor
--               * surface_factor
--               * lighting_factor
--               + crash_penalty
--               + traffic_control_penalty
--
-- Grain: one row per segment (way_id, start_position, end_position).
-- Direction-symmetric: forward and backward traversal of a segment
-- share the same stress_cost. The graph exporter handles direction.

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

{% set crash_weight = 24.0 %}
{% set crash_decay_lambda = 0.4 %}
{% set crash_buffer_degrees = 0.0002 %}

with segments as (
    select * from {{ ref('stg_chicago_bike_segments') }}
),
crashes as (
    select * from {{ ref('chicago_bike_crash_hotspots') }}
),

-- Crash deduplication: assign each crash to exactly one segment.
-- Priority: nearest by distance, then most dangerous highway class.
crash_nearest as (
    select distinct on (c.crash_record_id)
        c.crash_record_id,
        c.severity_score,
        c.crash_date,
        s.segment_id
    from crashes c
    inner join segments s
        on s.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, s.geom, {{ crash_buffer_degrees }})
    order by c.crash_record_id,
        ST_Distance(c.geom, s.geom),
        case
            when s.highway in ('primary', 'primary_link')       then 1
            when s.highway like '%primary%'                     then 1
            when s.highway in ('secondary', 'secondary_link')   then 2
            when s.highway like '%secondary%'                   then 2
            when s.highway in ('tertiary', 'tertiary_link')     then 3
            when s.highway like '%tertiary%'                    then 3
            when s.highway in ('unclassified')                  then 4
            when s.highway in ('residential')                   then 5
            when s.highway like '%residential%'                 then 5
            when s.highway in ('service')                       then 6
            else 7
        end
),
crash_scores as (
    select
        cn.segment_id,
        count(*) as crash_count,
        sum(
            power(cn.severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - cn.crash_date))
                / (365.25 * 86400)
            )
        ) as crash_score
    from crash_nearest cn
    group by cn.segment_id
),

factors as (
    select
        s.*,

        coalesce(cs.crash_count, 0) as crash_count,
        coalesce(cs.crash_score, 0) as crash_score,
        case
            when s.length_m > 0
            then coalesce(cs.crash_score, 0) / s.length_m
            else 0
        end as crash_score_per_meter,

        -- Speed factor
        case
            when s.highway in ('cycleway', 'path', 'footway', 'bridleway',
                               'pedestrian', 'living_street')                  then 1.0
            when s.highway like '%cycleway%' or s.highway like '%path%'        then 1.0
            when s.maxspeed is not null
                and regexp_replace(s.maxspeed, '[^0-9].*', '') ~ '^\d+$'
            then case
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 20 then 1.0
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 25 then 1.3
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 30 then 1.6
                else 2.0
            end
            when s.highway in ('service', 'residential', 'unclassified')       then 1.3
            when s.highway like '%residential%'                                then 1.3
            when s.highway in ('tertiary', 'tertiary_link')                    then 1.5
            when s.highway like '%tertiary%'                                   then 1.5
            when s.highway in ('secondary', 'secondary_link', 'busway')        then 1.7
            when s.highway like '%secondary%'                                  then 1.7
            when s.highway in ('primary', 'primary_link')                      then 2.0
            when s.highway like '%primary%'                                    then 2.0
            else 1.4
        end as speed_factor,

        -- Road type factor
        case
            when s.highway in ('cycleway')                      then 1.0
            when s.highway like '%cycleway%'                    then 1.0
            when s.highway in ('path', 'footway', 'bridleway')  then 1.2
            when s.highway like '%path%'                        then 1.2
            when s.highway in ('pedestrian')                    then 1.5
            when s.highway in ('living_street')                 then 3.0
            when s.highway in ('residential')                   then 3.0
            when s.highway like '%residential%'                 then 3.0
            when s.highway in ('unclassified')                  then 3.5
            when s.highway in ('busway')                        then 3.5
            when s.highway in ('service')                       then 6.0
            when s.highway in ('tertiary', 'tertiary_link')     then 5.5
            when s.highway like '%tertiary%'                    then 5.5
            when s.highway in ('secondary', 'secondary_link')   then 7.3
            when s.highway like '%secondary%'                   then 7.3
            when s.highway in ('primary', 'primary_link')       then 9.0
            when s.highway like '%primary%'                     then 9.0
            else 1.3
        end as road_type_factor,

        -- Infrastructure factor (no Socrata fallback in segment-grain pipeline)
        case
            when s.infra_type = 'protected_lane'    then 1.0
            when s.infra_type = 'track'             then 1.0
            when s.infra_type = 'shared_path'       then 1.3
            when s.infra_type = 'buffered_lane'     then 1.5
            when s.infra_type = 'designated_path'   then 1.5
            when s.infra_type = 'bicycle_road'      then 1.7
            when s.infra_type = 'bike_lane'         then 1.85
            when s.infra_type = 'share_busway'      then 2.0
            when s.infra_type = 'sharrow'           then 2.0
            else 2.5
        end as infrastructure_factor,

        -- Tunnel factor
        case
            when s.tunnel = 'yes'
                and coalesce(s.infra_type, '') not in (
                    'protected_lane', 'track', 'shared_path', 'buffered_lane'
                )
            then 2.5
            else 1.0
        end as tunnel_factor,

        -- Surface factor
        case
            when s.surface in ('asphalt', 'paved', 'concrete',
                               'concrete:plates', 'concrete:lanes') then 1.0
            when s.surface in ('paving_stones', 'sett',
                               'cobblestone', 'unhewn_cobblestone') then 1.3
            when s.surface in ('unpaved', 'gravel', 'fine_gravel',
                               'compacted', 'dirt', 'grass',
                               'ground', 'mud', 'sand')             then 1.5
            when s.surface is null                                  then 1.0
            else 1.0
        end as surface_factor,

        -- Lighting factor
        case
            when s.lit = 'yes' then 1.0
            when s.lit = 'no'  then 1.2
            else                    1.1
        end as lighting_factor,

        coalesce(ntc.traffic_control_penalty, 0) as traffic_control_penalty

    from segments s
    left join crash_scores cs on cs.segment_id = s.segment_id
    left join {{ ref('stg__chicago_traffic_control_nodes') }} ntc
        on ntc.osmid = s.end_node_id
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

    -- Crash features
    crash_count,
    crash_score,
    crash_score_per_meter,

    -- Intermediate stress factors (preserved for tuning)
    speed_factor,
    road_type_factor,
    infrastructure_factor,
    tunnel_factor,
    surface_factor,
    lighting_factor,
    traffic_control_penalty,

    crash_score_per_meter * length_m * {{ crash_weight }} as crash_penalty,

    greatest(
        length_m
            * speed_factor
            * road_type_factor
            * infrastructure_factor
            * tunnel_factor
            * surface_factor
            * lighting_factor
            + (crash_score_per_meter * length_m * {{ crash_weight }})
            + traffic_control_penalty,
        0
    ) as stress_cost

from factors
