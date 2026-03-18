-- bike_commuter__weighted_edges.sql
-- Enriches OSMnx bike network edges with infrastructure quality from the
-- infrastructure mart and time-decayed crash scores from crash hotspots.
--
-- This is the core model for safety-weighted bike routing. Each edge gets:
--   - infra_type, infra_category, and quality_tier from the nearest
--     infrastructure segment
--   - a time-decayed crash score that weights recent crashes more heavily
--   - crash_score_per_meter normalized by edge length
--
-- The Python graph builder reads this table to construct the weighted
-- NetworkX graph used for routing.
--
-- Performance notes:
--   - Uses geometry-based (planar) distance rather than geography (geodesic)
--     for spatial joins. At Chicago's latitude (~41.8°), 0.0004 degrees ≈
--     30-33m, which is accurate to within ~1m. This is dramatically faster
--     because it can use GiST indexes without on-the-fly coordinate transforms.
--   - Requires GiST spatial indexes on the source tables. Add post_hook
--     index creation to bike_crash_hotspots and
--     chicago_bike_routes if not already present.
--
-- Depends on:
--   {{ ref('chicago_bike_routes') }}
--   {{ ref('bike_crash_hotspots') }}
--   raw_data.osmnx_bike_network_edges

{{ config(
    materialized='table',
    pre_hook=["SET work_mem = '1024MB'"],
    post_hook=[
        "SET work_mem = '4MB'",
        "CREATE INDEX IF NOT EXISTS ix_{{ this.name }}_geom ON {{ this }} USING GIST (geom)",
        "CREATE INDEX IF NOT EXISTS ix_{{ this.name }}_uvk ON {{ this }} (u, v, key)"
    ]
) }}

{% set crash_decay_lambda = 0.4 %}

-- At Chicago's latitude (~41.8°):
--   1 degree latitude  ≈ 111,000 m
--   1 degree longitude ≈  82,700 m
-- We use 0.0003-0.0004 degrees as buffers (~25-33m).
{% set infra_buffer_degrees = 0.0003 %}
{% set crash_buffer_degrees = 0.0004 %}

with current_edges as materialized (
    select
        u,
        v,
        key,
        osmid,
        name,
        highway,
        oneway,
        length_m,
        maxspeed,
        surface,
        cycleway,
        cycleway_right,
        cycleway_left,
        bicycle,
        lanes,
        lit,
        bridge,
        tunnel,
        geom
    from raw_data.osmnx_bike_network_edges
    where valid_to is null
),

-- Quality tier ranking (1 = safest, 9 = least safe):
--   1: Dedicated cycleway (off-road, fully separated)
--   2: Protected lane (on-road, physically separated)
--   3: Dedicated shared path / shared footway (off-road, shared with peds)
--   4: Buffered sharrow (shared lane but with physical buffer)
--   5: Neighborhood greenway (traffic-calmed residential)
--   6: Shoulder / shared busway / other
--   7: Buffered lane (on-road, painted buffer)
--   8: Unbuffered bike lane (on-road, paint only)
--   9: Unbuffered sharrow (shared lane marking only)
-- Edges with no infrastructure match get tier 10.
infra_ranked as materialized (
    select
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
        end as quality_tier,
        row_number() over (
            partition by e.u, e.v, e.key
            order by
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
        ) as rank
    from current_edges e
    inner join {{ ref('chicago_bike_routes') }} i
        on i.geom && ST_Expand(e.geom, {{ infra_buffer_degrees }})
        and ST_DWithin(e.geom, i.geom, {{ infra_buffer_degrees }})
),

infra_match as (
    select
        u, v, key,
        infra_type,
        infra_category,
        quality_tier,
        has_buffer,
        infra_data_source
    from infra_ranked
    where rank = 1
),

-- Aggregate crash scores per edge using a simple spatial join.
--
-- Instead of snapping each crash to its single nearest edge (expensive
-- with 663k edges), we find all edges within the buffer of each crash
-- and aggregate. A crash near an intersection will count toward all
-- nearby edges — this is desirable for routing since all approaches
-- to a dangerous intersection should be penalized.
--
-- The decay formula: severity * exp(-lambda * years_since_crash)
-- With lambda = 0.4:
--   1 year ago  → 67% weight
--   2 years ago → 45% weight
--   5 years ago → 14% weight
--  10 years ago →  2% weight
crash_scores as materialized (
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
    from current_edges e
    inner join {{ ref('bike_crash_hotspots') }} c
        on c.geom && ST_Expand(e.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, e.geom, {{ crash_buffer_degrees }})
    group by e.u, e.v, e.key
)

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

    -- Infrastructure enrichment
    im.infra_type as matched_infra_type,
    im.infra_category as matched_infra_category,
    coalesce(im.quality_tier, 10) as quality_tier,
    im.has_buffer as matched_has_buffer,
    im.infra_data_source,

    -- Crash enrichment
    coalesce(cs.crash_count, 0) as crash_count,
    coalesce(cs.crash_score, 0) as crash_score,
    case
        when e.length_m > 0
        then coalesce(cs.crash_score, 0) / e.length_m
        else 0
    end as crash_score_per_meter

from current_edges e
left join infra_match im
    on e.u = im.u and e.v = im.v and e.key = im.key
left join crash_scores cs
    on e.u = cs.u and e.v = cs.v and e.key = cs.key