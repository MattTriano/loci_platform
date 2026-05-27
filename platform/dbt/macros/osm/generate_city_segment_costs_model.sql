{#
    Per-segment intrinsic stress costs derived from the physical attributes of
    the segment (highway class, infrastructure, surface, etc).
    Only OSM data required.

    Source: stg_<city>_bike_segments
    Grain: one row per segment (segment_id).

    Outputs:
      - All segment passthrough columns from stg_<city>_bike_segments
      - Six factor columns (preserved for tuning visibility):
          speed_factor, road_type_factor, infrastructure_factor,
          tunnel_factor, surface_factor, lighting_factor
      - physical_cost = length_m * product of the six factors

    Parameters:
      city: city name; used to resolve stg_<city>_bike_segments.
#}
{% macro generate_city_segment_costs_model(city) %}

with factors as (
    select
        s.*,

        -- Speed factor
        case
            when s.highway in ('cycleway', 'path', 'footway', 'bridleway',
                               'pedestrian', 'living_street')                  then 1.0
            when s.highway like '%cycleway%' or s.highway like '%path%'        then 1.0
            when s.maxspeed is not null
                and regexp_replace(s.maxspeed, '[^0-9].*', '') ~ '^\d+$'
            then case
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 20 then 1.0
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 25 then 1.3
                when regexp_replace(s.maxspeed, '[^0-9].*', '')::int <= 30 then 1.6
                else 2.0
            end
            when s.highway in ('service', 'residential', 'unclassified')       then 1.3
            when s.highway like '%residential%'                                then 1.3
            when s.highway in ('tertiary', 'tertiary_link')                    then 1.5
            when s.highway like '%tertiary%'                                   then 1.5
            when s.highway in ('secondary', 'secondary_link', 'busway')        then 1.7
            when s.highway like '%secondary%'                                  then 1.7
            when s.highway in ('primary', 'primary_link')                      then 2.0
            when s.highway like '%primary%'                                    then 2.0
            else 1.4
        end as speed_factor,

        -- Road type factor
        case
            when s.highway in ('cycleway')                      then 0.5
            when s.highway like '%cycleway%'                    then 0.5
            when s.highway in ('path', 'footway', 'bridleway')  then 0.7
            when s.highway like '%path%'                        then 0.7
            when s.highway in ('pedestrian')                    then 0.8
            when s.highway in ('living_street')                 then 1.3
            when s.highway in ('residential')                   then 1.3
            when s.highway like '%residential%'                 then 1.3
            when s.highway in ('unclassified')                  then 2.5
            when s.highway in ('busway')                        then 2.0
            when s.highway in ('service')                       then 4.0
            when s.highway in ('tertiary', 'tertiary_link')     then 3.0
            when s.highway like '%tertiary%'                    then 3.0
            when s.highway in ('secondary', 'secondary_link')   then 4.0
            when s.highway like '%secondary%'                   then 4.0
            when s.highway in ('primary', 'primary_link')       then 5.0
            when s.highway like '%primary%'                     then 5.0
            else 1.3
        end as road_type_factor,

        -- Infrastructure factor
        case
            when s.infra_type = 'protected_lane'    then 0.6
            when s.infra_type = 'track'             then 0.6
            when s.infra_type = 'shared_path'       then 0.8
            when s.infra_type = 'buffered_lane'     then 1.0
            when s.infra_type = 'designated_path'   then 1.0
            when s.infra_type = 'bicycle_road'      then 1.2
            when s.infra_type = 'bike_lane'         then 1.3
            when s.infra_type = 'share_busway'      then 1.5
            when s.infra_type = 'sharrow'           then 1.5
            else 2.5
        end as infrastructure_factor,

        -- Tunnel factor
        case
            when s.tunnel = 'yes'
                and coalesce(s.infra_type, '') not in (
                    'protected_lane', 'track', 'shared_path', 'buffered_lane'
                )
            then 2.5
            else 1.0
        end as tunnel_factor,

        -- Surface factor
        case
            when s.surface in ('asphalt', 'paved', 'concrete',
                               'concrete:plates', 'concrete:lanes') then 1.0
            when s.surface in ('paving_stones', 'sett',
                               'cobblestone', 'unhewn_cobblestone') then 1.3
            when s.surface in ('unpaved', 'gravel', 'fine_gravel',
                               'compacted', 'dirt', 'grass',
                               'ground', 'mud', 'sand')             then 1.5
            when s.surface is null                                  then 1.0
            else 1.0
        end as surface_factor,

        -- Lighting factor
        case
            when s.lit = 'yes' then 1.0
            when s.lit = 'no'  then 1.2
            else                    1.1
        end as lighting_factor

    from {{ ref('stg_' ~ city ~ '_bike_segments') }} s
)

select
    -- Identity
    segment_id,
    way_id,
    start_position,
    end_position,

    -- Topology
    start_node_id,
    end_node_id,
    direction,

    -- Geometry and length
    geom,
    length_m,

    -- Identification
    name,
    highway,

    -- Bike infrastructure
    infra_category,
    infra_type,
    has_buffer,

    -- Physical conditions
    surface,
    lit,
    bridge,
    tunnel,
    maxspeed,

    -- Raw cycleway tags
    cycleway,
    cycleway_left,
    cycleway_right,

    -- Factors (preserved for tuning)
    speed_factor,
    road_type_factor,
    infrastructure_factor,
    tunnel_factor,
    surface_factor,
    lighting_factor,

    length_m * (
        1 + (
            speed_factor
            * road_type_factor
            * infrastructure_factor
            * tunnel_factor
            * surface_factor
            * lighting_factor
        )
    ) as physical_cost

from factors

{% endmacro %}
