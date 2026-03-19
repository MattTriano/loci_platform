-- bike_safety_weighted_edges.sql
-- Enriches OSMnx bike network edges with infrastructure quality and
-- time-decayed crash scores for safety-weighted routing.
--
-- Performance strategy:
--   Only ~140k of 663k edges have any infra or crash match. We enrich
--   those via small-table-driven spatial joins (fast index probes), then
--   union in the remaining ~524k edges with default values. This avoids
--   ever scanning 663k edges as the driving table in a spatial join.
-- post_hook=[
    --     "RESET work_mem",
    --     "CREATE INDEX IF NOT EXISTS ix_{{ this.name }}_uvk ON {{ this }} (u, v, key)"
    -- ]

{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '256MB'"],
    post_hook=[
        "CREATE INDEX ON {{ this }} (u, v, key)",
        "RESET work_mem"
    ]
) }}

{% set crash_decay_lambda = 0.4 %}
{% set infra_buffer_degrees = 0.0003 %}
{% set crash_buffer_degrees = 0.0004 %}

with
-- =====================================================================
-- Step 1: Infrastructure match (driven from 11.6k infra → edge index)
-- =====================================================================
infra_match as (
    select distinct on (e.u, e.v, e.key)
        e.u,
        e.v,
        e.key,
        i.infra_type,
        i.infra_category,
        i.has_buffer,
        i.data_source as infra_data_source,
        case
            when i.infra_category = 'dedicated' and i.infra_type = 'cycleway' then 1
            when i.infra_type = 'protected_lane' then 2
            when i.infra_category = 'dedicated' and i.infra_type in ('shared_path', 'shared_footway') then 3
            when i.infra_type = 'sharrow' and i.has_buffer then 4
            when i.infra_type = 'neighborhood_greenway' then 5
            when i.infra_type in ('shoulder', 'shared_busway', 'other') then 6
            when i.infra_type = 'buffered_lane' then 7
            when i.infra_type = 'bike_lane' and not coalesce(i.has_buffer, false) then 8
            when i.infra_type = 'sharrow' and not coalesce(i.has_buffer, false) then 9
            else 6
        end as quality_tier
    from {{ ref('chicago_bike_routes') }} i
    inner join {{ source('raw_data', 'osmnx_bike_network_edges') }} e
        on e.geom && ST_Expand(i.geom, {{ infra_buffer_degrees }})
        and ST_DWithin(e.geom, i.geom, {{ infra_buffer_degrees }})
        and e.valid_to is null
    order by e.u, e.v, e.key,
        case
            when i.infra_category = 'dedicated' and i.infra_type = 'cycleway' then 1
            when i.infra_type = 'protected_lane' then 2
            when i.infra_category = 'dedicated' and i.infra_type in ('shared_path', 'shared_footway') then 3
            when i.infra_type = 'sharrow' and i.has_buffer then 4
            when i.infra_type = 'neighborhood_greenway' then 5
            when i.infra_type in ('shoulder', 'shared_busway', 'other') then 6
            when i.infra_type = 'buffered_lane' then 7
            when i.infra_type = 'bike_lane' and not coalesce(i.has_buffer, false) then 8
            when i.infra_type = 'sharrow' and not coalesce(i.has_buffer, false) then 9
            else 6
        end,
        ST_Distance(e.geom, i.geom)
),

-- =====================================================================
-- Step 2: Crash scores (driven from 17.5k crashes → edge index)
-- =====================================================================
crash_scores as (
    select
        e.u,
        e.v,
        e.key,
        count(*) as crash_count,
        sum(
            c.severity_score * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - c.crash_date)) / (365.25 * 86400)
            )
        ) as crash_score
    from {{ ref('bike_crash_hotspots') }} c
    inner join {{ source('raw_data', 'osmnx_bike_network_edges') }} e
        on e.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ crash_buffer_degrees }})
        and e.valid_to is null
    group by e.u, e.v, e.key
),

-- =====================================================================
-- Step 3: Collect all enriched edge keys
-- =====================================================================
enriched_keys as (
    select u, v, key from infra_match
    union
    select u, v, key from crash_scores
),

-- =====================================================================
-- Step 4a: Enriched edges — have infra and/or crash data
-- =====================================================================
enriched_edges as (
    select
        e.u,
        e.v,
        e.key,
        e.osmid,
        e.name,
        e.highway,
        e.oneway,
        e.length_m,
        e.maxspeed,
        e.surface,
        e.cycleway,
        e.cycleway_right,
        e.cycleway_left,
        e.bicycle,
        e.lanes,
        e.lit,
        e.bridge,
        e.tunnel,
        e.geom,
        im.infra_type as matched_infra_type,
        im.infra_category as matched_infra_category,
        coalesce(im.quality_tier, 10) as quality_tier,
        im.has_buffer as matched_has_buffer,
        im.infra_data_source,
        coalesce(cs.crash_count, 0) as crash_count,
        coalesce(cs.crash_score, 0) as crash_score,
        case
            when e.length_m > 0
            then coalesce(cs.crash_score, 0) / e.length_m
            else 0
        end as crash_score_per_meter
    from enriched_keys ek
    inner join {{ source('raw_data', 'osmnx_bike_network_edges') }} e
        on e.u = ek.u and e.v = ek.v and e.key = ek.key
        and e.valid_to is null
    left join infra_match im
        on ek.u = im.u and ek.v = im.v and ek.key = im.key
    left join crash_scores cs
        on ek.u = cs.u and ek.v = cs.v and ek.key = cs.key
),

-- =====================================================================
-- Step 4b: Plain edges — no infra or crash match, just defaults
-- =====================================================================
plain_edges as (
    select
        e.u,
        e.v,
        e.key,
        e.osmid,
        e.name,
        e.highway,
        e.oneway,
        e.length_m,
        e.maxspeed,
        e.surface,
        e.cycleway,
        e.cycleway_right,
        e.cycleway_left,
        e.bicycle,
        e.lanes,
        e.lit,
        e.bridge,
        e.tunnel,
        e.geom,
        null::text as matched_infra_type,
        null::text as matched_infra_category,
        10 as quality_tier,
        null::boolean as matched_has_buffer,
        null::text as infra_data_source,
        0 as crash_count,
        0::double precision as crash_score,
        0::double precision as crash_score_per_meter
    from {{ source('raw_data', 'osmnx_bike_network_edges') }} e
    where e.valid_to is null
        and not exists (
            select 1 from enriched_keys ek
            where ek.u = e.u and ek.v = e.v and ek.key = e.key
        )
)

select * from enriched_edges
union all
select * from plain_edges