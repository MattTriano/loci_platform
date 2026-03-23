-- bike_safety_weighted_edges.sql
-- Assigns a safety cost to each bikeable OSMnx edge for use in
-- safety-weighted routing.
--
-- Cost formula:
--   safety_cost = length_m
--               * speed_factor
--               * infrastructure_factor
--               * surface_factor
--               * lighting_factor
--               + crash_penalty
--
-- crash_penalty = crash_score_per_meter * length_m * crash_weight
-- crash_score uses severity^2 * time-decay, summed per edge.
--
-- All tunable weights are jinja variables at the top of this file.
-- Intermediate factors are preserved as columns for inspection and tuning.
--
-- Grain: one row per directed edge (u, v, key).
-- Excludes edges unsuitable for cycling (trunks, parking aisles,
-- underground tunnels, emergency bays).

{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{ this }} (u, v, key)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "RESET work_mem"
    ]
) }}

{% set crash_weight = 2.0 %}
{% set crash_decay_lambda = 0.4 %}
{% set crash_buffer_degrees = 0.0004 %}

with edges as (
    select *
    from {{ ref('chicago_bike_network_edges') }}
    -- where
    --     -- Trunk roads (highways, Lower Wacker) and their links
        -- highway != 'closed'
        -- and (name is null or name not ilike '% lower%')
    --     highway not in ('trunk', 'trunk_link', 'closed', 'emergency_bay')
    --     and highway not like '%trunk%'

    -- --     -- Vehicle-only service roads (keep alley, null, emergency_access)
    -- --     and (
    -- --         service is null
    -- --         or not (
    -- --             service like '%parking_aisle%'
    -- --             or service like '%driveway%'
    -- --             or service like '%drive-through%'
    -- --         )
    -- --     )
    -- --     -- Covered/underground tunnels (Lower Wacker and similar)
    --     and (name is null or name not ilike '% lower%')
),

crashes as (
    select * from {{ ref('bike_crash_hotspots') }}
),

-- =====================================================================
-- Crash scores: driven from small crashes table onto edge spatial index.
-- Severity is squared per crash before summing to amplify the difference
-- between fatal and minor crashes.
-- =====================================================================
crash_scores as (
    select
        e.u,
        e.v,
        e.key,
        count(*)                                        as crash_count,
        sum(
            power(c.severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - c.crash_date))
                / (365.25 * 86400)
            )
        )                                               as crash_score
    from crashes c
    inner join edges e
        on e.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ crash_buffer_degrees }})
    group by e.u, e.v, e.key
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

        -- Infrastructure factor: separation from traffic is the strongest
        -- safety signal. Socrata display_route_type takes precedence over
        -- OSMnx highway type since it reflects actual installed infrastructure.
        case
            -- Socrata-matched infrastructure
            when e.display_route_type = 'Protected Bike Lane'   then 0.5
            when e.display_route_type = 'Neighborhood Greenway' then 0.7
            when e.display_route_type = 'Buffered Bike Lane'    then 0.8
            when e.display_route_type = 'Bike Lane'             then 1.0
            when e.display_route_type = 'Marked Shared Lane'    then 1.3
            -- OSMnx highway type fallback (no Socrata match)
            when e.highway in ('cycleway')                      then 0.6
            when e.highway like '%cycleway%'                    then 0.6
            when e.highway in ('path')                          then 0.7
            when e.highway like '%path%'                        then 0.7
            when e.highway in ('living_street')                 then 1.0
            when e.highway in ('pedestrian')                    then 1.0
            when e.highway in ('residential')                   then 1.1
            when e.highway like '%residential%'                 then 1.1
            when e.highway in ('unclassified')                  then 1.2
            when e.highway in ('busway')                        then 1.2
            when e.highway in ('service')
                 and e.service = 'alley'                        then 0.9
            when e.highway in ('service')                       then 1.1
            when e.highway in ('tertiary', 'tertiary_link')     then 1.4
            when e.highway like '%tertiary%'                    then 1.4
            when e.highway in ('secondary', 'secondary_link')   then 1.7
            when e.highway like '%secondary%'                   then 1.7
            when e.highway in ('primary', 'primary_link')       then 2.2
            when e.highway like '%primary%'                     then 2.2
            else 1.5
        end                             as infrastructure_factor,

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
        end                             as lighting_factor

    from edges e
    left join crash_scores cs using (u, v, key)
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

        -- Bike infrastructure
        cycleway,
        cycleway_right,
        cycleway_left,
        bicycle,
        display_route_type,
        soc_infra_type,
        bike_route_oneway,
        bike_route_oneway_dir,

        -- Physical conditions
        surface,
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
        infrastructure_factor,
        surface_factor,
        lighting_factor,

        -- Crash penalty (additive, scaled by length and weight)
        crash_score_per_meter * length_m * {{ crash_weight }}
                                as crash_penalty,

        -- Final safety cost
        length_m
            * speed_factor
            * infrastructure_factor
            * surface_factor
            * lighting_factor
            + (crash_score_per_meter * length_m * {{ crash_weight }})
                                as safety_cost

    from factors
)

select * from final
