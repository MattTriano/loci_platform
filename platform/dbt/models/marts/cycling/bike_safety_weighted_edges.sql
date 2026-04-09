-- bike_safety_weighted_edges.sql
-- Assigns a safety cost to each bikeable OSMnx edge for use in
-- safety-weighted routing.
--
-- Cost formula:
--   safety_cost = length_m
--               * speed_factor
--               * road_type_factor
--               * infrastructure_factor
--               * surface_factor
--               * lighting_factor
--               + crash_penalty
--
-- crash_penalty = crash_score_per_meter * length_m * crash_weight
-- crash_score uses severity^2 * time-decay, summed per edge.
--
-- road_type_factor captures the inherent danger of the road environment
-- (traffic volume, road design, turning movements) based on highway class.
--
-- infrastructure_factor captures how much bike infrastructure mitigates
-- that danger. Uses OSM-derived infra_type as primary signal (from
-- cycleway tags we actively maintain), falling back to Socrata
-- display_route_type. Edges with no bike infra get a neutral 1.0.
--
-- The two factors multiply: a sharrow on a residential (1.1 * 1.3 = 1.43)
-- costs far less than a sharrow on a primary (2.2 * 1.3 = 2.86).
--
-- All tunable weights are jinja variables at the top of this file.
-- Intermediate factors are preserved as columns for inspection and tuning.
--
-- Grain: one row per directed edge (u, v, key).

{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{ this }} (u, v, key)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "RESET work_mem"
    ]
) }}

{% set crash_weight = 4.0 %}
{% set crash_decay_lambda = 0.4 %}
{% set crash_buffer_degrees = 0.0002 %}

with edges as (
    select *
    from {{ ref('chicago_bike_network_edges') }}
),
crashes as (
    select * from {{ ref('bike_crash_hotspots') }}
),
-- =====================================================================
-- Crash deduplication: assign each crash to exactly one edge.
-- Priority: most dangerous highway class, then nearest by distance.
-- This prevents a single crash near an intersection from being counted
-- on multiple short segments.
-- =====================================================================
crash_nearest as (
    select distinct on (c.crash_record_id)
        c.crash_record_id,
        c.severity_score,
        c.crash_date,
        e.u, e.v, e.key
    from crashes c
    inner join edges e
        on e.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ crash_buffer_degrees }})
    order by c.crash_record_id,
        case
            when e.highway in ('primary', 'primary_link')       then 1
            when e.highway like '%primary%'                     then 1
            when e.highway in ('secondary', 'secondary_link')   then 2
            when e.highway like '%secondary%'                   then 2
            when e.highway in ('tertiary', 'tertiary_link')     then 3
            when e.highway like '%tertiary%'                    then 3
            when e.highway in ('unclassified')                  then 4
            when e.highway in ('residential')                   then 5
            when e.highway like '%residential%'                 then 5
            when e.highway in ('service')                       then 6
            else 7
        end,
        ST_Distance(c.geom, e.geom)
),
-- =====================================================================
-- Crash scores: aggregate deduplicated crashes per edge.
-- Severity is squared per crash before summing to amplify the difference
-- between fatal and minor crashes.
-- =====================================================================
crash_scores as (
    select
        cn.u,
        cn.v,
        cn.key,
        count(*)                                        as crash_count,
        sum(
            power(cn.severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - cn.crash_date))
                / (365.25 * 86400)
            )
        )                                               as crash_score
    from crash_nearest cn
    group by cn.u, cn.v, cn.key
),

-- =====================================================================
-- Factor computation
-- =====================================================================
factors as (
    select
        e.*,

        coalesce(cs.crash_count, 0)     as crash_count,
        coalesce(cs.crash_score, 0)     as crash_score,
        case
            when e.length_m > 0
            then coalesce(cs.crash_score, 0) / e.length_m
            else 0
        end                             as crash_score_per_meter,

        -- Speed factor: proxy for injury severity if a collision occurs.
        -- maxspeed values can be "30 mph", "48", "30 mph;50 mph" etc.
        -- We extract the first numeric value and treat unknown as arterial.
        case
            when e.maxspeed is null                                         then 1.4
            when regexp_replace(e.maxspeed, '[^0-9].*', '') ~ '^\d+$'
                and regexp_replace(e.maxspeed, '[^0-9].*', '')::int <= 20  then 1.0
            when regexp_replace(e.maxspeed, '[^0-9].*', '') ~ '^\d+$'
                and regexp_replace(e.maxspeed, '[^0-9].*', '')::int <= 25  then 1.3
            when regexp_replace(e.maxspeed, '[^0-9].*', '') ~ '^\d+$'
                and regexp_replace(e.maxspeed, '[^0-9].*', '')::int <= 30  then 1.6
            when regexp_replace(e.maxspeed, '[^0-9].*', '') ~ '^\d+$'
                and regexp_replace(e.maxspeed, '[^0-9].*', '')::int  > 30  then 2.0
            else 1.4
        end                             as speed_factor,

        -- Road type factor: inherent danger of the road environment
        -- independent of bike infrastructure. Based on highway class as
        -- a proxy for traffic volume, road design, and turning conflicts.
        case
            when e.highway in ('cycleway')                      then 0.6
            when e.highway like '%cycleway%'                    then 0.6
            when e.highway in ('path', 'footway', 'bridleway')  then 0.7
            when e.highway like '%path%'                        then 0.7
            when e.highway in ('pedestrian')                    then 0.8
            when e.highway in ('living_street')                 then 0.9
            when e.highway = 'service' and e.service = 'alley'  then 1.3
            when e.highway in ('service')                       then 1.0
            when e.highway in ('residential')                   then 1.1
            when e.highway like '%residential%'                 then 1.1
            when e.highway in ('unclassified')                  then 1.4
            when e.highway in ('busway')                        then 1.4
            when e.highway in ('tertiary', 'tertiary_link')     then 1.7
            when e.highway like '%tertiary%'                    then 1.7
            when e.highway in ('secondary', 'secondary_link')   then 2.2
            when e.highway like '%secondary%'                   then 2.2
            when e.highway in ('primary', 'primary_link')       then 2.7
            when e.highway like '%primary%'                     then 2.7
            else 1.3
        end                             as road_type_factor,

        -- Infrastructure factor: how much bike infrastructure mitigates
        -- the road's inherent danger. Edges with no bike infra get 1.0
        -- (neutral — road_type_factor alone determines their cost).
        --
        -- Priority: OSM infra_type (actively maintained tags) > Socrata
        -- display_route_type > neutral fallback.
        case
            -- 1. OSM-derived infra_type (primary signal)
            when e.osm_infra_type = 'protected_lane'    then 0.4
            when e.osm_infra_type = 'track'             then 0.5
            when e.osm_infra_type = 'shared_path'       then 0.6
            when e.osm_infra_type = 'buffered_lane'     then 0.7
            when e.osm_infra_type = 'designated_path'   then 0.7
            when e.osm_infra_type = 'bicycle_road'      then 0.8
            when e.osm_infra_type = 'bike_lane'         then 0.85
            when e.osm_infra_type = 'share_busway'      then 0.9
            when e.osm_infra_type = 'sharrow'           then 0.95

            -- 2. Socrata display_route_type (fallback)
            when e.display_route_type = 'Protected Bike Lane'   then 0.4
            when e.display_route_type = 'Neighborhood Greenway' then 0.6
            when e.display_route_type = 'Buffered Bike Lane'    then 0.7
            when e.display_route_type = 'Bike Lane'             then 0.85
            when e.display_route_type = 'Marked Shared Lane'    then 0.95

            -- 3. No bike infrastructure — neutral
            else 1.0
        end                             as infrastructure_factor,

        -- Tunnel factor: penalizes tunnels without protective bike infrastructure.
        -- infrastructure_factor <= 0.7 indicates at least a buffered lane or better.
        case
            when e.tunnel = 'yes'
                and coalesce(e.osm_infra_type, '') not in (
                    'protected_lane', 'track', 'shared_path', 'buffered_lane'
                )
                and coalesce(e.display_route_type, '') not in (
                    'Protected Bike Lane', 'Neighborhood Greenway', 'Buffered Bike Lane'
                )
            then 2.5
            else 1.0
        end as tunnel_factor,

        -- Surface factor: affects control and crash risk.
        case
            when e.surface in ('asphalt', 'paved', 'concrete',
                               'concrete:plates', 'concrete:lanes') then 1.0
            when e.surface in ('paving_stones', 'sett',
                               'cobblestone', 'unhewn_cobblestone') then 1.3
            when e.surface in ('unpaved', 'gravel', 'fine_gravel',
                               'compacted', 'dirt', 'grass',
                               'ground', 'mud', 'sand')             then 1.5
            when e.surface is null                                  then 1.0
            else 1.0
        end                             as surface_factor,

        -- Lighting factor: affects nighttime safety.
        case
            when e.lit = 'yes'          then 1.0
            when e.lit = 'no'           then 1.2
            else                             1.1  -- unknown
        end                             as lighting_factor,
        coalesce(ntc.traffic_control_penalty, 0) as traffic_control_penalty

    from edges e
    left join crash_scores cs using (u, v, key)
    left join {{ ref('stg__traffic_control_nodes') }} as ntc
        on ntc.osmid = e.v
),

-- =====================================================================
-- Final cost assembly
-- =====================================================================
final as (
    select
        -- Graph topology
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

        -- Bike infrastructure: OSM classification
        osm_infra_category,
        osm_infra_type,
        osm_has_buffer,

        -- Bike infrastructure: OSM raw tags
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

        -- Bike infrastructure: Socrata enrichment
        display_route_type,
        soc_infra_type,
        bike_route_oneway,
        bike_route_oneway_dir,

        -- Physical conditions
        surface,
        cycleway_surface,
        cycleway_smoothness,
        lit,
        bridge,
        tunnel,

        -- Geometry and distance
        length_m,
        geom,

        -- Crash features
        crash_count,
        crash_score,
        crash_score_per_meter,

        -- Intermediate safety factors (preserved for tuning)
        speed_factor,
        road_type_factor,
        infrastructure_factor,
        tunnel_factor,
        surface_factor,
        lighting_factor,
        traffic_control_penalty,

        -- Crash penalty (additive, scaled by length and weight)
        crash_score_per_meter * length_m * {{ crash_weight }}
                                as crash_penalty,

        -- Final safety cost
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
        ) as safety_cost
    from factors
)

select * from final
