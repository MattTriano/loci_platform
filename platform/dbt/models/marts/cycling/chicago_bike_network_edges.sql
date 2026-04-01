-- chicago_bike_network_edges.sql
-- OSMnx bike network edges enriched with Chicago Data Portal route info
-- and OSM-derived infrastructure classification.
--
-- Grain: one row per directed OSMnx edge (u, v, key).
-- Each edge is matched to the safest Socrata bike route that runs along it,
-- using endpoint proximity, street name similarity, and direction compatibility
-- as match signals. Each edge is also joined to the OSM-derived infra_type
-- from stg__osm__bike_infrastructure.
--
-- Match logic:
--   Hard filter : both endpoints of the OSMnx edge are within ~10m of the
--                 Socrata route geometry (is_endpoints_near = true).
--   Soft signals: street name match (is_name_match) and direction
--                 compatibility (is_direction_compatible) are used for
--                 ranking only, not filtering.
--   Ranking     : among candidates passing the hard filter, pick the safest
--                 display_route_type, then prefer name matches, then direction
--                 matches.
--
-- Unmatched edges are kept with null Socrata/infra columns.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

{% set dupe_buffer_degrees = 0.00015 %}

with edges as (
    select * from {{ ref('stg__osmnx__bike_network_edges') }}
),

osm_infra as (
    select
        segment_id,
        infra_category  as osm_infra_category,
        infra_type      as osm_infra_type,
        has_buffer       as osm_has_buffer
    from {{ ref('stg__osm__bike_infrastructure') }}
),

soc_routes as (
    select * from {{ ref('stg__soc__bike_infrastructure') }}
),

-- Candidate matches: OSMnx edges spatially near a Socrata route.
-- One edge may match many routes at this stage.
candidates as (
    select
        e.u,
        e.v,
        e.key,
        s.route_segment_id,
        s.display_route_type,
        s.infra_type         as soc_infra_type,
        s.street_name        as soc_street_name,
        s.bike_route_oneway,
        s.bike_route_oneway_dir,

        -- Hard filter signal: both endpoints near the Socrata route
        ST_DWithin(
            ST_StartPoint(e.geom),
            s.geom,
            {{ dupe_buffer_degrees }}
        ) and ST_DWithin(
            ST_EndPoint(e.geom),
            s.geom,
            {{ dupe_buffer_degrees }}
        ) as is_endpoints_near,

        -- Soft signal: street name overlap
        (
            e.name ilike '%' || s.street_name || '%'
            or s.street_name ilike '%' || coalesce(e.name, '') || '%'
        ) as is_name_match,

        -- Soft signal: direction compatibility.
        case
            when s.bike_route_oneway = '2W'                       then true
            when s.bike_route_oneway_dir in ('-', null)           then null
            when s.bike_route_oneway_dir = 'N'
                and degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) between 315 and 360
                or  degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) < 45  then true
            when s.bike_route_oneway_dir = 'S'
                and degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) between 135 and 225 then true
            when s.bike_route_oneway_dir = 'E'
                and degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) between 45  and 135 then true
            when s.bike_route_oneway_dir = 'W'
                and degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) between 225 and 315 then true
            when s.bike_route_oneway_dir = 'NW'
                and degrees(ST_Azimuth(ST_StartPoint(e.geom), ST_EndPoint(e.geom))) between 270 and 360 then true
            else false
        end as is_direction_compatible,

        -- Safety rank: lower = safer
        case s.display_route_type
            when 'Protected Bike Lane'   then 1
            when 'Neighborhood Greenway' then 2
            when 'Buffered Bike Lane'    then 3
            when 'Bike Lane'             then 4
            when 'Marked Shared Lane'    then 5
            else                              6
        end as safety_rank

    from edges e
    join soc_routes s
        on s.geom && ST_Expand(e.geom, {{ dupe_buffer_degrees }})
        and ST_DWithin(e.geom, s.geom, {{ dupe_buffer_degrees }})
),

ranked as (
    select
        u, v, key,
        route_segment_id,
        display_route_type,
        soc_infra_type,
        soc_street_name,
        bike_route_oneway,
        bike_route_oneway_dir,
        is_name_match,
        is_direction_compatible,
        row_number() over (
            partition by u, v, key
            order by
                safety_rank asc,
                is_name_match desc,
                coalesce(is_direction_compatible, true) desc
        ) as rn
    from candidates
    where is_endpoints_near
),

best_match as (
    select * from ranked where rn = 1
),

final as (
    select
        -- Graph topology
        e.u,
        e.v,
        e.key,
        e.segment_id,
        e.osmid,

        -- Road attributes
        e.name,
        e.highway,
        e.oneway,
        e.reversed,
        e.lanes,
        e.ref,
        e.service,
        e.width,
        e.maxspeed,
        e.access,

        -- Bike infrastructure: OSM-derived classification
        oi.osm_infra_category,
        oi.osm_infra_type,
        oi.osm_has_buffer,

        -- Bike infrastructure: OSM raw tags (for downstream refinement)
        e.cycleway,
        e.cycleway_right,
        e.cycleway_left,
        e.cycleway_both,
        e.cycleway_separation,
        e.cycleway_right_separation,
        e.cycleway_left_separation,
        e.cycleway_both_separation,
        e.bicycle,
        e.class_bicycle,
        e.bicycle_road,
        e.cyclestreet,

        -- Bike infrastructure: Socrata enrichment
        m.display_route_type,
        m.soc_infra_type,
        m.bike_route_oneway,
        m.bike_route_oneway_dir,

        -- Physical conditions
        e.surface,
        e.cycleway_surface,
        e.cycleway_smoothness,
        e.lit,
        e.bridge,
        e.tunnel,

        -- Geometry and distance
        e.length_m,
        e.geom

    from edges e
    left join osm_infra oi on oi.segment_id = e.segment_id
    left join best_match m using (u, v, key)
)

select * from final
