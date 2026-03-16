-- stg__osm__bike_infrastructure.sql
-- Bike-relevant infrastructure from OSM ways: dedicated cycleways, bike lanes,
-- shared paths, and roads with cycleway tags.
--
-- This captures three categories:
--   1. Dedicated cycling ways (highway=cycleway, highway=path with bicycle=designated)
--   2. Roads with bike lane annotations (cycleway=* tags on regular roads)
--   3. Bicycle streets / designated bike roads (cyclestreet=yes, bicycle_road=yes)
--
-- Tags reference: https://wiki.openstreetmap.org/wiki/Bicycle

with dedicated_ways as (
    -- Standalone cycleways and multi-use paths
    select
        osm_id,
        geom,
        tags->>'name' as name,
        tags->>'highway' as highway_type,
        'dedicated' as infra_category,
        case
            when tags->>'highway' = 'cycleway' then 'cycleway'
            when tags->>'highway' = 'path'
                 and tags->>'bicycle' in ('designated', 'yes') then 'shared_path'
            when tags->>'highway' = 'footway'
                 and tags->>'bicycle' = 'yes' then 'shared_footway'
            when tags->>'highway' = 'pedestrian'
                 and tags->>'bicycle' in ('designated', 'yes') then 'shared_footway'
        end as infra_type,
        tags->>'surface' as surface,
        tags->>'lit' as is_lit,
        tags->>'oneway' as oneway,
        tags->>'width' as width,
        null as maxspeed,
        false as has_buffer,
        tags as all_tags
    from raw_data.osm_ways
    where
        (
            tags->>'highway' = 'cycleway'
            or (tags->>'highway' = 'path' and tags->>'bicycle' in ('designated', 'yes'))
            or (tags->>'highway' = 'footway' and tags->>'bicycle' = 'yes')
            or (tags->>'highway' = 'pedestrian' and tags->>'bicycle' in ('designated', 'yes'))
        )
        and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
        and valid_to is null
),
roads_with_bike_infra as (
    -- Regular roads that have cycleway annotations.
    --
    -- We resolve the cycleway value from whichever key is present, preferring
    -- the more specific :right/:left keys over the generic cycleway key.
    -- We exclude 'no' (explicitly no infra) and 'separate' (infra is on an
    -- adjacent dedicated way, already captured above).
    select
        osm_id,
        geom,
        tags->>'name' as name,
        tags->>'highway' as highway_type,
        'on_road' as infra_category,

        -- Resolve the cycleway value from the first non-null, non-exclusion key.
        -- This gives us one representative infra type per road segment.
        coalesce(
            nullif(tags->>'cycleway:right', 'no'),
            nullif(tags->>'cycleway:left', 'no'),
            nullif(tags->>'cycleway:both', 'no'),
            nullif(tags->>'cycleway', 'no')
        ) as _raw_cycleway_value,

        tags->>'surface' as surface,
        tags->>'lit' as is_lit,
        tags->>'oneway' as oneway,
        tags->>'width' as width,
        tags->>'maxspeed' as maxspeed,

        -- Check for buffer tags on any side
        (
            tags ? 'cycleway:buffer'
            or tags ? 'cycleway:both:buffer'
            or tags ? 'cycleway:right:buffer'
            or tags ? 'cycleway:left:buffer'
        ) as has_buffer,

        tags as all_tags
    from raw_data.osm_ways
    where
        tags->>'highway' in (
            'primary', 'secondary', 'tertiary', 'residential',
            'unclassified', 'living_street', 'service'
        )
        and (
            tags ? 'cycleway'
            or tags ? 'cycleway:both'
            or tags ? 'cycleway:right'
            or tags ? 'cycleway:left'
        )
        and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
        and valid_to is null
),
roads_classified as (
    -- Map raw cycleway values to standardized infra_type, and filter out
    -- rows where the resolved value is null, 'no', or 'separate'.
    select
        osm_id,
        geom,
        name,
        highway_type,
        infra_category,
        case
            when _raw_cycleway_value in ('track', 'separated')
                then 'protected_lane'
            when _raw_cycleway_value = 'lane' and has_buffer
                then 'buffered_lane'
            when _raw_cycleway_value = 'lane'
                then 'bike_lane'
            when _raw_cycleway_value in ('opposite_lane', 'opposite_track')
                then case
                    when _raw_cycleway_value = 'opposite_track' then 'protected_lane'
                    else 'bike_lane'
                end
            when _raw_cycleway_value = 'shared_lane'
                then 'sharrow'
            when _raw_cycleway_value = 'share_busway'
                then 'shared_busway'
            when _raw_cycleway_value in ('shared', 'soft_lane')
                then 'sharrow'
            when _raw_cycleway_value = 'shoulder'
                then 'shoulder'
            else 'other'
        end as infra_type,
        surface,
        is_lit,
        oneway,
        width,
        maxspeed,
        has_buffer,
        all_tags
    from roads_with_bike_infra
    where
        -- Exclude roads where the resolved value indicates no bike infra
        -- or that the infra is mapped as a separate way
        _raw_cycleway_value is not null
        and _raw_cycleway_value not in ('no', 'separate')
),
bike_streets as (
    -- Bicycle streets / bicycle roads: residential streets where bikes
    -- have priority. Maps to neighborhood_greenway in our taxonomy.
    select
        osm_id,
        geom,
        tags->>'name' as name,
        tags->>'highway' as highway_type,
        'on_road' as infra_category,
        'neighborhood_greenway' as infra_type,
        tags->>'surface' as surface,
        tags->>'lit' as is_lit,
        tags->>'oneway' as oneway,
        tags->>'width' as width,
        tags->>'maxspeed' as maxspeed,
        false as has_buffer,
        tags as all_tags
    from raw_data.osm_ways
    where
        (tags->>'cyclestreet' = 'yes' or tags->>'bicycle_road' = 'yes')
        and geom && ST_MakeEnvelope(-87.97, 41.62, -87.5, 42.05, 4326)
        and valid_to is null
        -- Exclude ways already captured as dedicated cycleways
        and tags->>'highway' not in ('cycleway', 'path', 'footway')
        -- Exclude roads already captured in roads_with_bike_infra
        and not (
            tags ? 'cycleway'
            or tags ? 'cycleway:both'
            or tags ? 'cycleway:right'
            or tags ? 'cycleway:left'
        )
),
unified as (
    select
        osm_id,
        geom,
        name,
        highway_type,
        infra_category,
        infra_type,
        surface,
        is_lit,
        oneway,
        width,
        maxspeed,
        has_buffer,
        all_tags
    from dedicated_ways
    union all
    select
        osm_id,
        geom,
        name,
        highway_type,
        infra_category,
        infra_type,
        surface,
        is_lit,
        oneway,
        width,
        maxspeed,
        has_buffer,
        all_tags
    from roads_classified
    union all
    select
        osm_id,
        geom,
        name,
        highway_type,
        infra_category,
        infra_type,
        surface,
        is_lit,
        oneway,
        width,
        maxspeed,
        has_buffer,
        all_tags
    from bike_streets
)

select * from unified
