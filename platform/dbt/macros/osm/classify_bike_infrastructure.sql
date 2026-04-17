{% macro classify_bike_infrastructure(edges_relation) %}
{#
    Classifies OSMnx bike network edges into infrastructure categories
    using standard OSM cycleway/bicycle tags.

    Args:
        edges_relation: a relation with the standard OSMnx edge columns,
            including at minimum: highway, bicycle, bicycle_road, cyclestreet,
            cycleway, cycleway_left, cycleway_right, cycleway_both,
            cycleway_buffer, cycleway_left_buffer, cycleway_right_buffer,
            cycleway_both_buffer, cycleway_separation, cycleway_left_separation,
            cycleway_right_separation, cycleway_both_separation,
            cycleway_shared_lane, cycleway_left_shared_lane,
            cycleway_right_shared_lane, cycleway_both_shared_lane.

    Returns three columns:
        osm_infra_category: 'separated', 'on_road', 'shared', 'path', or null
        osm_infra_type:     'protected_lane', 'track', 'buffered_lane',
                            'bike_lane', 'sharrow', 'bicycle_road',
                            'share_busway', 'shared_path', 'designated_path',
                            or null
        osm_has_buffer:     true if any buffer tag is present

    Usage:
        with edges as (
            select * from {{ ref('stg__osmnx_chicago_bike_network_edges') }}
        ),
        classified as (
            {{ classify_bike_infrastructure('edges') }}
        )
        select * from classified
#}

select
    __resolved.*,

    -- Infrastructure category
    case
        when __resolved.highway in ('cycleway', 'path', 'pedestrian')
            then 'separated'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('track', 'separate')
            then 'separated'
        when __resolved.__eff_separation is not null
            and __resolved.__eff_separation not in ('no', 'none')
            then 'separated'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('lane', 'exclusive')
            then 'on_road'
        when __resolved.__osm_has_buffer
            then 'on_road'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in (
            'shared_lane', 'shared', 'share_busway'
        )
            then 'shared'
        when __resolved.__eff_shared_lane is not null
            then 'shared'
        when __resolved.bicycle_road = 'yes' or __resolved.cyclestreet = 'yes'
            then 'shared'
        when __resolved.highway in ('footway', 'bridleway', 'steps')
            and __resolved.bicycle in ('yes', 'designated', 'permissive')
            then 'path'
        when __resolved.bicycle in ('designated', 'yes')
            then 'shared'
        else null
    end as osm_infra_category,

    -- Infrastructure type (specific)
    case
        when __resolved.__eff_separation is not null
            and __resolved.__eff_separation not in ('no', 'none')
            and coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('track', 'lane', 'separate')
            then 'protected_lane'
        when __resolved.highway = 'cycleway'
            then 'track'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('track', 'separate')
            then 'track'
        when __resolved.__osm_has_buffer
            then 'buffered_lane'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('lane', 'exclusive')
            then 'bike_lane'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') in ('shared_lane', 'shared')
            then 'sharrow'
        when __resolved.__eff_shared_lane is not null
            then 'sharrow'
        when __resolved.bicycle_road = 'yes' or __resolved.cyclestreet = 'yes'
            then 'bicycle_road'
        when coalesce(__resolved.__eff_cycleway_right, __resolved.__eff_cycleway_left, '') = 'share_busway'
            then 'share_busway'
        when __resolved.highway in ('path', 'footway', 'bridleway', 'pedestrian', 'steps')
            then 'shared_path'
        when __resolved.bicycle = 'designated'
            then 'designated_path'
        else null
    end as osm_infra_type

from (
    select
        __edges.*,

        coalesce(__edges.cycleway_left, __edges.cycleway_both, __edges.cycleway)
            as __eff_cycleway_left,
        coalesce(__edges.cycleway_right, __edges.cycleway_both, __edges.cycleway)
            as __eff_cycleway_right,

        coalesce(
            __edges.cycleway_left_buffer,
            __edges.cycleway_right_buffer,
            __edges.cycleway_both_buffer,
            __edges.cycleway_buffer
        ) is not null as __osm_has_buffer,

        coalesce(
            __edges.cycleway_left_separation,
            __edges.cycleway_right_separation,
            __edges.cycleway_both_separation,
            __edges.cycleway_separation
        ) as __eff_separation,

        coalesce(
            __edges.cycleway_left_shared_lane,
            __edges.cycleway_right_shared_lane,
            __edges.cycleway_both_shared_lane,
            __edges.cycleway_shared_lane
        ) as __eff_shared_lane

    from {{ edges_relation }} as __edges
) as __resolved

{% endmacro %}