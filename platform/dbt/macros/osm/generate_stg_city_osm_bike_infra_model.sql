{#
    Classifies bike network segments into infrastructure categories
    using the full set of cycleway/bicycle tags.

    Source: stg_<city>_way_segments
    Grain: one row per segment with bike infrastructure present.

    Infrastructure taxonomy:
      infra_category: 'separated', 'on_road', 'shared', 'path'
      infra_type:     'protected_lane', 'track', 'buffered_lane',
                      'bike_lane', 'sharrow', 'bicycle_road',
                      'share_busway', 'shared_path', 'designated_path'

    The classification logic resolves left/right/both cycleway tags into
    a single per-segment classification. When left and right differ, we
    use the higher-quality side (separated > on_road > shared).

    Parameters:
      city: city name; used to resolve the upstream stg_<city>_way_segments ref.
      include_all_sidewalks: when true, plain (untagged) footways are
        classified as 'path' infrastructure. Use for cities where
        sidewalk riding is legal. When false (default), only footways
        with an explicit bicycle=yes/designated/permissive tag are
        included.
#}
{% macro generate_stg_city_osm_bike_infra_model(city, include_all_sidewalks=false) %}

with segments as (
    select * from {{ ref('stg_' ~ city ~ '_way_segments') }}
),

resolved as (
    select
        *,
        coalesce(cycleway_left, cycleway_both, cycleway) as eff_cycleway_left,
        coalesce(cycleway_right, cycleway_both, cycleway) as eff_cycleway_right,
        coalesce(
            cycleway_left_buffer,
            cycleway_right_buffer,
            cycleway_both_buffer,
            cycleway_buffer
        ) is not null as has_any_buffer,
        coalesce(
            cycleway_left_separation,
            cycleway_right_separation,
            cycleway_both_separation,
            cycleway_separation
        ) as eff_separation,
        coalesce(
            cycleway_left_shared_lane,
            cycleway_right_shared_lane,
            cycleway_both_shared_lane,
            cycleway_shared_lane
        ) as eff_shared_lane
    from segments
),

classified as (
    select
        way_id,
        start_position,
        end_position,
        start_node_id,
        end_node_id,

        case
            when highway in ('cycleway', 'path', 'pedestrian')
                then 'separated'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'separate')
                then 'separated'
            when eff_separation is not null
                and eff_separation not in ('no', 'none')
                then 'separated'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('lane', 'exclusive')
                then 'on_road'
            when has_any_buffer
                then 'on_road'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in (
                'shared_lane', 'shared', 'share_busway'
            )
                then 'shared'
            when eff_shared_lane is not null
                then 'shared'
            when bicycle_road = 'yes' or cyclestreet = 'yes'
                then 'shared'
            when highway in ('footway', 'bridleway', 'steps')
                and (
                    bicycle in ('yes', 'designated', 'permissive')
                    {% if include_all_sidewalks %}or bicycle is null{% endif %}
                )
                then 'path'
            when bicycle in ('designated', 'yes')
                then 'shared'
            else null
        end as infra_category,

        case
            when eff_separation is not null
                and eff_separation not in ('no', 'none')
                and coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'lane', 'separate')
                then 'protected_lane'
            when highway = 'cycleway'
                then 'track'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('track', 'separate')
                then 'track'
            when has_any_buffer
                then 'buffered_lane'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('lane', 'exclusive')
                then 'bike_lane'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') in ('shared_lane', 'shared')
                then 'sharrow'
            when eff_shared_lane is not null
                then 'sharrow'
            when bicycle_road = 'yes' or cyclestreet = 'yes'
                then 'bicycle_road'
            when coalesce(eff_cycleway_right, eff_cycleway_left, '') = 'share_busway'
                then 'share_busway'
            when highway in ('path', 'footway', 'bridleway', 'pedestrian', 'steps')
                then 'shared_path'
            when bicycle = 'designated'
                then 'designated_path'
            else null
        end as infra_type,

        has_any_buffer as has_buffer

    from resolved
)

select
    way_id,
    start_position,
    end_position,
    start_node_id,
    end_node_id,
    infra_category,
    infra_type,
    has_buffer
from classified
where infra_category is not null

{% endmacro %}
