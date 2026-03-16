-- mart_bike_infrastructure.sql
-- Unified bike infrastructure from OSM and Chicago data portal.
--
-- Standardizes both sources into a common schema with a shared infra_type
-- taxonomy. Flags likely duplicates using spatial proximity + street name
-- matching rather than dropping them, so we can review and decide later.

{% set dupe_buffer_meters = 10 %}

with osm_infra as (
    select
        osm_id::text as source_id,
        geom,
        name as street_name,
        infra_category,
        infra_type,
        highway_type,
        surface,
        is_lit,
        oneway,
        has_buffer,
        'osm' as data_source
    from {{ ref('stg__osm__bike_infrastructure') }}
),

chicago_infra as (
    select
        route_segment_id as source_id,
        geom,
        street_name,
        'on_road' as infra_category,
        infra_type,
        null as highway_type,
        null as surface,
        null as is_lit,
        oneway_dir as oneway,
        infra_type = 'buffered_lane' as has_buffer,
        'chicago_data_portal' as data_source
    from {{ ref('stg__soc__bike_infrastructure') }}
),

combined as (
    select * from osm_infra
    union all
    select * from chicago_infra
),

-- Flag likely duplicates: a Chicago segment is "likely duplicate" if an OSM
-- segment is nearby and shares a street name token.
--
-- We flag Chicago rows that match an OSM row, since OSM tends to have richer
-- metadata (surface, lighting, etc). But we keep both so you can review.
--
-- is_endpoints_near refines the flag by checking that BOTH endpoints of the
-- Chicago segment are close to the OSM segment. This filters out false
-- positives from perpendicular streets that merely cross at an intersection.
duplicate_flags as (
    select
        c.source_id,
        c.data_source,
        bool_or(
            ST_DWithin(
                c.geom::geography,
                o.geom::geography,
                {{ dupe_buffer_meters }}
            )
            and (
                o.street_name ilike '%' || c.street_name || '%'
                or c.street_name ilike '%' || coalesce(o.street_name, '') || '%'
                or o.street_name is null
            )
        ) as is_likely_duplicate,
        -- Both endpoints of the Chicago segment are within the buffer
        -- distance of the OSM segment. A cross street will only touch
        -- the OSM way at one end (or the middle), not both ends.
        bool_or(
            ST_DWithin(
                ST_StartPoint(ST_GeometryN(c.geom, 1))::geography,
                o.geom::geography,
                {{ dupe_buffer_meters }}
            )
            and ST_DWithin(
                ST_EndPoint(ST_GeometryN(c.geom, 1))::geography,
                o.geom::geography,
                {{ dupe_buffer_meters }}
            )
            and (
                o.street_name ilike '%' || c.street_name || '%'
                or c.street_name ilike '%' || coalesce(o.street_name, '') || '%'
                or o.street_name is null
            )
        ) as is_endpoints_near
    from chicago_infra c
    left join osm_infra o
        on o.geom && ST_Expand(c.geom, 0.0003)
        and ST_DWithin(c.geom::geography, o.geom::geography, {{ dupe_buffer_meters }})
    group by c.source_id, c.data_source
),
dupe_labeling as (
    select
        combined.*,
        case
            when combined.data_source = 'osm' then false
            else coalesce(df.is_likely_duplicate, false)
        end as is_likely_duplicate,
        case
            when combined.data_source = 'osm' then false
            else coalesce(df.is_endpoints_near, false)
        end as is_endpoints_near
    from combined
    left join duplicate_flags df
        on combined.source_id = df.source_id
        and combined.data_source = df.data_source
)

select
    source_id,
    geom,
    street_name,
    infra_category,
    infra_type,
    highway_type,
    surface,
    is_lit,
    oneway,
    has_buffer,
    data_source
from dupe_labeling
where not (is_likely_duplicate and is_endpoints_near)
