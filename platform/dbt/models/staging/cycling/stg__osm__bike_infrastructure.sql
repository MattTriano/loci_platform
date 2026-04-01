-- stg__osm__bike_infrastructure.sql
-- Classifies OSMnx bike network edges into infrastructure categories
-- using the full set of cycleway/bicycle tags.
--
-- Source: stg__osmnx__bike_network_edges
-- Grain: one row per directed edge with bike infrastructure present.
--
-- Infrastructure taxonomy:
--   infra_category: 'separated', 'on_road', 'shared', 'path'
--   infra_type: more specific (e.g. 'protected_lane', 'buffered_lane',
--               'bike_lane', 'shared_lane', 'sharrow', 'track',
--               'shared_path', 'designated_path')
--
-- The classification logic resolves left/right/both cycleway tags into
-- a single per-edge classification. When left and right differ, we use
-- the higher-quality side (separated > on_road > shared).

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

with edges as (
    select * from {{ ref('stg__osmnx__bike_network_edges') }}
),

-- Resolve the effective cycleway value for each edge. OSM tags can appear
-- on cycleway, cycleway:left, cycleway:right, or cycleway:both. We
-- coalesce into a single effective value per side, then pick the best.
resolved as (
    select
        *,
        -- Effective cycleway value per side
        coalesce(cycleway_left, cycleway_both, cycleway) as eff_cycleway_left,
        coalesce(cycleway_right, cycleway_both, cycleway) as eff_cycleway_right,
        -- Buffer present on either side
        coalesce(
            cycleway_left_buffer,
            cycleway_right_buffer,
            cycleway_both_buffer,
            cycleway_buffer
        ) is not null as has_any_buffer,
        -- Separation present on either side
        coalesce(
            cycleway_left_separation,
            cycleway_right_separation,
            cycleway_both_separation,
            cycleway_separation
        ) as eff_separation,
        -- Shared lane marking on either side
        coalesce(
            cycleway_left_shared_lane,
            cycleway_right_shared_lane,
            cycleway_both_shared_lane,
            cycleway_shared_lane
        ) as eff_shared_lane
    from edges
),
classified as (
    select
        segment_id,
        u,
        v,
        key,
        osmid,
        name,
        highway,
        geom,
        length_m,
        surface,
        lit is not null and lit != 'no' as is_lit,
        oneway,

        -- Derive the best cycleway value (prefer the higher-quality side)
        coalesce(eff_cycleway_right, eff_cycleway_left) as eff_cycleway,
        has_any_buffer,
        eff_separation,
        eff_shared_lane,
        bicycle,
        class_bicycle,
        bicycle_road,
        cyclestreet,

        -- Infrastructure type classification
        case
            -- Separated: physically separated from motor traffic
            when highway in ('cycleway', 'path', 'pedestrian')
                then 'separated'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'separate')
                then 'separated'
            when eff_separation is not null
                and eff_separation not in ('no', 'none')
                then 'separated'
            -- On-road: marked lane on the roadway
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('lane', 'exclusive')
                then 'on_road'
            when has_any_buffer
                then 'on_road'
            -- Shared: shared with traffic but explicitly designated
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in (
                'shared_lane', 'shared', 'share_busway'
            )
                then 'shared'
            when eff_shared_lane is not null
                then 'shared'
            when bicycle_road = 'yes' or cyclestreet = 'yes'
                then 'shared'
            -- Path: off-road shared-use paths
            when highway in ('footway', 'bridleway', 'steps')
                and bicycle in ('yes', 'designated', 'permissive')
                then 'path'
            -- Fallback: if bicycle=designated on a road, treat as shared
            when bicycle in ('designated', 'yes')
                then 'shared'
            else null
        end as infra_category,
        case
            -- Protected: physical separation (bollards, curb, planters, etc.)
            when eff_separation is not null
                and eff_separation not in ('no', 'none')
                and coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'lane', 'separate')
                then 'protected_lane'
            -- Track: fully separated cycleway
            when highway = 'cycleway'
                then 'track'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'separate')
                then 'track'
            -- Buffered lane: painted buffer but no physical separation
            when has_any_buffer
                then 'buffered_lane'
            -- Bike lane: standard painted lane
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('lane', 'exclusive')
                then 'bike_lane'
            -- Sharrow / shared lane marking
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('shared_lane', 'shared')
                then 'sharrow'
            when eff_shared_lane is not null
                then 'sharrow'
            -- Bicycle road / cycle street
            when bicycle_road = 'yes' or cyclestreet = 'yes'
                then 'bicycle_road'
            -- Share busway
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') = 'share_busway'
                then 'share_busway'
            -- Shared-use path
            when highway in ('path', 'footway', 'bridleway', 'pedestrian', 'steps')
                then 'shared_path'
            -- Designated on a road
            when bicycle = 'designated'
                then 'designated_path'
            else null
        end as infra_type
    from resolved
),
-- Only keep edges that have some form of bike infrastructure
filtered as (
    select *
    from classified
    where infra_category is not null
),
final as (
    select
        segment_id,
        osmid as osm_id,
        geom,
        name as street_name,
        infra_category,
        infra_type,
        highway as highway_type,
        surface,
        is_lit,
        oneway,
        has_any_buffer as has_buffer
    from filtered
)

select * from final
