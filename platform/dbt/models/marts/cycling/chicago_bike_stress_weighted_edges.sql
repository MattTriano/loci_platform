-- bike_stress_weighted_edges.sql
-- Assigns a stress cost to each bikeable OSMnx edge for use in
-- stress-weighted routing.
--
-- Cost formula:
--   stress_cost = length_m
--               * road_infra_factor      (matrix: road_class × infra_type)
--               * speed_factor
--               * tunnel_factor
--               * surface_factor
--               * lighting_factor
--               + crash_penalty
--               + traffic_control_penalty
--
-- road_infra_factor replaces the old separate road_type_factor and
-- infrastructure_factor. Those two were multiplied as if independent,
-- which under-rewarded good infrastructure on bad roads. The matrix
-- captures the fact that e.g. "bike lane on primary" is a different
-- experience than "bike lane on residential", not just the product of
-- generic road and generic bike-lane factors. See the macro for the
-- full matrix and calibration rationale.
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

{% set crash_weight = 10.0 %}
{% set crash_decay_lambda = 0.4 %}
{% set edge_crash_buffer_degrees = 0.0002 %}
{% set node_crash_radius_degrees = 0.00025 %}
{% set cycleway_crash_attenuation = 0.3 %}
{% set node_crash_weight = 10.0 %}

with edges as (
    select *
    from {{ ref('chicago_bike_network_edges') }}
),
crashes as (
    select * from {{ ref('chicago_bike_crash_hotspots') }}
),
-- -- =====================================================================
-- -- Crash deduplication: assign each crash to exactly one edge.
-- -- Priority: most dangerous highway class, then nearest by distance.
-- -- (Known issue: tracks drawn as separate geometries can win over the
-- -- adjacent arterial if they're closer to the crash point. Tracked in
-- -- findings doc as a follow-up.)
-- -- =====================================================================
-- crash_nearest as (
--     select distinct on (c.crash_record_id)
--         c.crash_record_id,
--         c.severity_score,
--         c.crash_date,
--         e.u, e.v, e.key
--     from crashes c
--     inner join edges e
--         on e.geom && ST_Expand(c.geom, {* {{ crash_buffer_degrees }}) *}
--         and ST_DWithin(c.geom, e.geom, {* {{ crash_buffer_degrees }}) *}
--     order by c.crash_record_id,
--         ST_Distance(c.geom, e.geom),
--         case
--             when e.highway in ('primary', 'primary_link')       then 1
--             when e.highway like '%primary%'                     then 1
--             when e.highway in ('secondary', 'secondary_link')   then 2
--             when e.highway like '%secondary%'                   then 2
--             when e.highway in ('tertiary', 'tertiary_link')     then 3
--             when e.highway like '%tertiary%'                    then 3
--             when e.highway in ('unclassified')                  then 4
--             when e.highway in ('residential')                   then 5
--             when e.highway like '%residential%'                 then 5
--             when e.highway in ('service')                       then 6
--             else 7
--         end
-- ),
-- crash_scores as (
--     select
--         cn.u,
--         cn.v,
--         cn.key,
--         count(*)                                        as crash_count,
--         sum(
--             power(cn.severity_score, 2) * exp(
--                 {* -{{ crash_decay_lambda }} *}
--                 * extract(epoch from (current_date - cn.crash_date))
--                 / (365.25 * 86400)
--             )
--         )                                               as crash_score
--     from crash_nearest cn
--     group by cn.u, cn.v, cn.key
-- ),

-- =====================================================================
-- Mid-block crash attribution.
--
-- A crash is "mid-block" if it isn't claimed by any intersection node
-- (i.e., it's further than 25m from any degree>=3 node). For those
-- crashes, attribute to the nearest edge with priority:
--   1. road class (most dangerous wins)
--   2. distance (nearest wins within same class)
--
-- Priority ordering is inverted from v1: road class comes BEFORE
-- distance. A cycleway drawn alongside a primary road will no longer
-- steal the primary's crashes just by being closer to the crash point.
--
-- The "walls" problem from the old road-class-first attribution was
-- mitigated by the intersection/mid-block split above: most crashes
-- happen at intersections and now attach to the node, so the mid-block
-- set is sparse and won't concentrate on short edges.
-- =====================================================================
mid_block_crashes as (
    select c.*
    from {{ ref('chicago_bike_crash_hotspots') }} c
    where not exists (
        select 1
        from {{ ref('stg__chicago_crash_node_penalties') }} ncp
        join {{ source('raw_data', 'osmnx_chicago_bike_network_nodes') }} n
            on n.osmid = ncp.osmid
           and n.valid_to is null
        where ST_DWithin(c.geom, n.geom, {{ node_crash_radius_degrees }})
    )
),
crash_edge_attribution as (
    select distinct on (c.crash_record_id)
        c.crash_record_id,
        c.severity_score,
        c.crash_date,
        e.u, e.v, e.key, e.highway
    from mid_block_crashes c
    inner join edges e
        on e.geom && ST_Expand(c.geom, {{ edge_crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ edge_crash_buffer_degrees }})
    order by c.crash_record_id,
        case
            when e.highway like '%primary%'     then 1
            when e.highway like '%secondary%'   then 2
            when e.highway like '%tertiary%'    then 3
            when e.highway = 'unclassified'     then 4
            when e.highway = 'residential'      then 5
            when e.highway = 'living_street'    then 5
            when e.highway = 'service'          then 6
            when e.highway = 'cycleway'         then 7
            when e.highway in ('path', 'footway', 'pedestrian', 'bridleway') then 8
            else 9
        end,
        ST_Distance(c.geom, e.geom)
),
edge_crash_scores as (
    select
        u, v, key,
        count(*) as crash_count,
        sum(
            power(severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - crash_date))
                / (365.25 * 86400)
            )
        ) as crash_score
    from crash_edge_attribution
    group by u, v, key
),

-- =====================================================================
-- Factor computation
-- =====================================================================
factors as (
    select
        e.*,

        coalesce(ecs.crash_count, 0)     as edge_crash_count,
        coalesce(ecs.crash_score, 0)     as edge_crash_score,
        case
            when e.length_m > 0
            then coalesce(ecs.crash_score, 0) / e.length_m
            else 0
        end                              as crash_score_per_meter,
        coalesce(ncp.node_crash_count, 0)  as node_crash_count,
        coalesce(ncp.node_crash_score, 0)  as node_crash_score,

        -- Road × infrastructure matrix factor (replaces the old
        -- road_type_factor × infrastructure_factor multiplication).
        {{ road_infra_stress_factor('e.highway', 'e.service', 'e.osm_infra_type') }}
                                        as road_infra_factor,

        -- Speed factor: proxy for injury severity if a collision occurs.
        -- maxspeed values can be "30 mph", "48", "30 mph;50 mph" etc.
        -- We extract the first numeric value and treat unknown as arterial.
        case
            when e.highway like '%cycleway%'                               then 1.0
            when e.maxspeed is null                                        then 1.4
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

        -- Tunnel factor: penalizes tunnels without protective bike infrastructure.
        case
            when e.tunnel = 'yes'
                and coalesce(e.osm_infra_type, '') not in (
                    'protected_lane', 'track', 'shared_path', 'buffered_lane'
                )
            then 2.5
            else 1.0
        end                             as tunnel_factor,

        -- Surface factor: affects control and comfort.
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

        -- Lighting factor: affects nighttime comfort.
        case
            when e.lit = 'yes'          then 1.0
            when e.lit = 'no'           then 1.2
            else                             1.1  -- unknown
        end                             as lighting_factor,

        coalesce(ntc.traffic_control_penalty, 0) as traffic_control_penalty

    from edges e
    left join edge_crash_scores ecs using (u, v, key)
    left join {{ ref('stg__chicago_traffic_control_nodes') }} as ntc
        on ntc.osmid = e.v
    left join {{ ref('stg__chicago_crash_node_penalties') }} as ncp
        on ncp.osmid = e.v  -- pay node crash penalty when arriving at v
),

-- =====================================================================
-- Final cost assembly
-- =====================================================================
final as (
    select
        -- Graph topology
        u, v, key, osmid,

        -- Road attributes
        name, highway, oneway, reversed, lanes, ref, service, width,
        maxspeed, access,

        -- Bike infrastructure: OSM classification
        osm_infra_category, osm_infra_type, osm_has_buffer,

        -- Bike infrastructure: OSM raw tags
        cycleway, cycleway_right, cycleway_left, cycleway_both,
        cycleway_separation, cycleway_right_separation,
        cycleway_left_separation, cycleway_both_separation,
        bicycle, class_bicycle, bicycle_road, cyclestreet,

        -- Physical conditions
        surface, cycleway_surface, cycleway_smoothness, lit, bridge, tunnel,

        -- Geometry and distance
        length_m, geom,

        -- Crash features
        edge_crash_count, edge_crash_score, crash_score_per_meter,
        node_crash_count, node_crash_score,

        -- Intermediate stress factors (preserved for inspection/tuning)
        road_infra_factor,
        speed_factor,
        tunnel_factor,
        surface_factor,
        lighting_factor,
        traffic_control_penalty,

        -- Edge crash penalty (additive, scaled by length, attenuated on
        -- highway=cycleway since crashes near a cycletrack usually reflect
        -- intersection risk rather than on-track risk).
        case
            when highway = 'cycleway'
                then crash_score_per_meter * length_m * {{ crash_weight }}
                     * {{ cycleway_crash_attenuation }}
            else     crash_score_per_meter * length_m * {{ crash_weight }}
        end as edge_crash_penalty,

        -- Node crash penalty (additive, paid once per transit into node).
        -- Not attenuated — a dangerous intersection is dangerous regardless
        -- of which approach you take.
        node_crash_score * {{ node_crash_weight }} as node_crash_penalty,

        -- Final stress cost
        greatest(
            length_m
                * road_infra_factor
                * speed_factor
                * tunnel_factor
                * surface_factor
                * lighting_factor
                + (
                    case
                        when highway = 'cycleway'
                            then crash_score_per_meter * length_m * {{ crash_weight }}
                                 * {{ cycleway_crash_attenuation }}
                        else     crash_score_per_meter * length_m * {{ crash_weight }}
                    end
                  )
                + (node_crash_score * {{ node_crash_weight }})
                + traffic_control_penalty,
            0
        ) as stress_cost
    from factors
)

select * from final
