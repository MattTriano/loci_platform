-- stg__osmnx_chicago_bike_network_edges.sql (revised)

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

with bounded as (
    select *
    from {{ source('raw_data', 'osmnx_chicago_bike_network_edges') }}
    where
        valid_to is null
        and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
        and u is not null
        and v is not null
        and key is not null
),
-- =====================================================================
-- Filter edges that contain any disallowed highway type.
--
-- This runs BEFORE compound-value resolution because a merged edge
-- that includes a trunk segment isn't rideable even if it also
-- includes a primary segment — some portion of its geometry is on
-- a limited-access highway. Same reasoning for 'closed'.
--
-- Disallowed types:
--   - trunk / trunk_link / motorway / motorway_link: bikes prohibited
--   - emergency_bay: not a routable way
--   - closed / construction / proposed / abandoned / razed / planned:
--     not a currently-usable way
--   - raceway: not a public road
-- =====================================================================
allowed as (
    select *
    from bounded
    where
        -- Plain single values
        highway not in (
            'trunk', 'trunk_link',
            'motorway', 'motorway_link',
            'emergency_bay', 'closed',
            'raceway', 'construction', 'proposed',
            'abandoned', 'razed', 'planned'
        )
        -- Compound list values — reject if the list contains any disallowed type
        and highway not like '%''trunk''%'
        and highway not like '%''trunk_link''%'
        and highway not like '%''motorway''%'
        and highway not like '%''motorway_link''%'
        and highway not like '%''emergency_bay''%'
        and highway not like '%''closed''%'
        and highway not like '%''raceway''%'
        and highway not like '%''construction''%'
        and highway not like '%''proposed''%'
        and highway not like '%''abandoned''%'
        and highway not like '%''razed''%'
        and highway not like '%''planned''%'
),
-- =====================================================================
-- Resolve compound highway values using bike-aware "most restrictive wins".
--
-- For bike routing, a merged edge that includes cycleway or path
-- segments is usually dominated by that bike-specific character —
-- most-restrictive-wins over-pessimizes these cases (e.g., Prairie
-- Path Lane with a brief residential transition).
--
-- But if the merged edge ALSO includes an arterial segment (primary,
-- secondary, tertiary), the arterial governs: you don't want to route
-- someone onto the arterial portion of a merged edge just because
-- most of the length is bike-friendly.
--
-- Priority (first match wins):
--   1. cycleway, if no arterial present
--   2. path, if no arterial present
--   3. most-restrictive remaining type
--
-- Compound edges are imperfect by nature — the validator flags them
-- as Category A issues so they can be untangled in OSM over time
-- (ideally by splitting the OSM ways at the type transition).
--
-- Original value preserved as highway_raw.
-- =====================================================================
resolved as (
    select
        *,
        highway as highway_raw,
        case
            when highway is null                      then null
            when highway not like '[%'                then highway

            -- Bike-specific wins when no arterial is in the mix.
            when highway like '%''cycleway''%'
                 and highway not like '%''primary''%'
                 and highway not like '%''secondary''%'
                 and highway not like '%''tertiary''%' then 'cycleway'
            when highway like '%''path''%'
                 and highway not like '%''primary''%'
                 and highway not like '%''secondary''%'
                 and highway not like '%''tertiary''%' then 'path'

            -- Otherwise most-restrictive wins (LTS weakest-link).
            when highway like '%''primary_link''%'    then 'primary_link'
            when highway like '%''primary''%'         then 'primary'
            when highway like '%''secondary_link''%'  then 'secondary_link'
            when highway like '%''secondary''%'       then 'secondary'
            when highway like '%''tertiary_link''%'   then 'tertiary_link'
            when highway like '%''tertiary''%'        then 'tertiary'
            when highway like '%''unclassified''%'    then 'unclassified'
            when highway like '%''residential''%'     then 'residential'
            when highway like '%''service''%'         then 'service'
            when highway like '%''living_street''%'   then 'living_street'
            when highway like '%''pedestrian''%'      then 'pedestrian'
            when highway like '%''track''%'           then 'track'
            when highway like '%''bridleway''%'       then 'bridleway'
            when highway like '%''footway''%'         then 'footway'
            else highway  -- fall through; validator will flag
        end as highway_resolved
    from allowed
),
final as (
    select
        md5(u || '|' || v || '|' || key) as segment_id,
        u, v, key, osmid,

        name,
        highway_resolved as highway,
        highway_raw,
        oneway, reversed, lanes, ref, service, width, maxspeed, access,

        bicycle, bicycle_lanes, bicycle_lanes_backward, bicycle_lanes_forward,
        bicycle_right, bicycle_road, class_bicycle, cyclestreet, oneway_bicycle,
        ramp_bicycle, sidewalk_both_bicycle,

        cycleway, cycleway_buffer, cycleway_lane, cycleway_oneway,
        cycleway_separation, cycleway_shared_lane, cycleway_smoothness,
        cycleway_surface,

        cycleway_both, cycleway_both_buffer, cycleway_both_colour,
        cycleway_both_lane, cycleway_both_separation, cycleway_both_shared_lane,
        cycleway_both_traffic_sign,

        cycleway_left, cycleway_left_buffer, cycleway_left_lane,
        cycleway_left_oneway, cycleway_left_separation,
        cycleway_left_shared_lane, cycleway_left_traffic_sign,

        cycleway_right, cycleway_right_buffer, cycleway_right_lane,
        cycleway_right_oneway, cycleway_right_separation,
        cycleway_right_shared_lane, cycleway_right_traffic_sign,

        surface, lit, bridge, tunnel,
        length_m, geom
    from resolved
)

select * from final