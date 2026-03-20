-- chicago_bike_network_edges_with_crashes.sql
-- Enriches bike network edges with time-decayed crash scores.
--
-- Source: mart__chicago_bike_network_edges (edges with Socrata enrichment)
--         bike_crash_hotspots (crash locations with severity scores)
--
-- The crash spatial join is expensive, so we drive it from the small
-- crashes table (~17.5k) onto the edge index, collect matched edge keys,
-- then union in the remaining edges with zero crash values.
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

{% set crash_decay_lambda = 0.4 %}
{% set crash_buffer_degrees = 0.0004 %}

with edges as (
    select * from {{ ref('chicago_bike_network_edges') }}
),

crashes as (
    select * from {{ ref('bike_crash_hotspots') }}
),

-- =====================================================================
-- Crash scores: driven from small crashes table onto edge spatial index
-- =====================================================================
crash_scores as (
    select
        e.u,
        e.v,
        e.key,
        count(*)                                    as crash_count,
        sum(
            c.severity_score * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - c.crash_date))
                / (365.25 * 86400)
            )
        )                                           as crash_score
    from crashes c
    inner join edges e
        on e.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ crash_buffer_degrees }})
    group by e.u, e.v, e.key
),

-- =====================================================================
-- Enriched edges: have at least one nearby crash
-- =====================================================================
enriched_edges as (
    select
        e.*,
        cs.crash_count,
        cs.crash_score,
        case
            when e.length_m > 0
            then cs.crash_score / e.length_m
            else 0
        end                                         as crash_score_per_meter
    from crash_scores cs
    inner join edges e using (u, v, key)
),

-- =====================================================================
-- Plain edges: no nearby crashes, zero defaults
-- =====================================================================
plain_edges as (
    select
        e.*,
        0                                           as crash_count,
        0::double precision                         as crash_score,
        0::double precision                         as crash_score_per_meter
    from edges e
    where not exists (
        select 1 from crash_scores cs
        where cs.u = e.u and cs.v = e.v and cs.key = e.key
    )
)

select * from enriched_edges
union all
select * from plain_edges
