{#
    Identifies "logical intersections" in a city's bike network and assigns each
    one a stress penalty representing the cost of a cyclist traversing it.

    A "logical intersection" is a node where at least two distinct OSM ways meet
    AND at least one of those ways carries cars. This captures the places where a
    cyclist's path crosses or merges with car traffic, which is the underlying
    stressor we want to penalize.

    Explicitly excluded:
      - Mid-block pedestrian crossings (only one way touches the node)
      - Cycleway-to-cycleway or path-to-path junctions (no car traffic)
      - Cul-de-sac endpoints, turning circles, turning loops
      - Mid-segment graph-split nodes

    Penalty design:
      - Direction-symmetric: the penalty is a property of the node, not
          the path through it. Turn-specific costs are handled separately
          in the routing algorithm.
      - Higher road class -> higher penalty (more/faster cars to cross).
      - Better traffic control -> lower penalty.

    Joined into <city>_bike_stress_weighted_segments on end_node_id.

#}
{% macro generate_stg_city_intersection_costs_model(city) %}
-- =====================================================================
-- Ways meeting at each node, with each way's highway class and a flag
-- for whether it carries cars. Distinct on (node, way) so a self-loop
-- counts a way only once.
-- =====================================================================
with ways_at_node as (
    select distinct
        wn.node_id,
        wn.way_id,
        w.highway,
        case
            when w.highway in (
                'primary', 'primary_link',
                'secondary', 'secondary_link',
                'tertiary', 'tertiary_link',
                'unclassified', 'residential', 'service', 'busway'
            ) then true
            when w.highway like '%primary%'     then true
            when w.highway like '%secondary%'   then true
            when w.highway like '%tertiary%'    then true
            when w.highway like '%residential%' then true
            else false
        end as is_car_carrying
    from {{ ref('stg_' ~ city ~ '_way_nodes') }} as wn
    inner join {{ ref('stg_' ~ city ~ '_bike_ways') }} as w
        on w.osm_id = wn.way_id
),
-- =====================================================================
-- Per-node summary: how many distinct ways meet, and the worst road
-- class among the *car-carrying* ways. Nodes with no car-carrying way
-- get max_road_rank = NULL and are filtered out next.
-- =====================================================================
node_summary as (
    select
        node_id,
        count(*) as way_count,
        max(case when is_car_carrying then
            case
                when highway in ('primary', 'primary_link')
                    or highway like '%primary%'         then 4
                when highway in ('secondary', 'secondary_link')
                    or highway like '%secondary%'       then 3
                when highway in ('tertiary', 'tertiary_link')
                    or highway like '%tertiary%'        then 2
                else                                         1
            end
        end) as max_road_rank
    from ways_at_node
    group by node_id
),
-- =====================================================================
-- Logical intersections: 2+ distinct ways AND >= 1 carries cars.
-- =====================================================================
logical_intersections as (
    select
        node_id,
        max_road_rank,
        case max_road_rank
            when 4 then 'primary'
            when 3 then 'secondary'
            when 2 then 'tertiary'
            else        'minor'
        end as max_road_class
    from node_summary
    where way_count >= 2
      and max_road_rank is not null
),
-- =====================================================================
-- Attach traffic control tag from the raw OSM nodes source.
-- Nodes missing from the source get NULL -> treated as uncontrolled.
-- =====================================================================
nodes_with_control as (
    select
        li.node_id as osmid,
        n.tags->>'highway' as traffic_control,
        li.max_road_rank,
        li.max_road_class
    from logical_intersections as li
    left join {{ source('osm', city ~ '_osm_bike_network_nodes') }} as n
        on n.osm_type = 'node'
        and n.osm_id = li.node_id
        and n.valid_to is null
)

select
    osmid,
    traffic_control,
    max_road_rank,
    max_road_class,
    case
        when traffic_control = 'traffic_signals' then
            case max_road_class
                when 'primary'   then 200.0
                when 'secondary' then 100.0
                when 'tertiary'  then 50.0
                else                  25.0
            end

        when traffic_control = 'stop' then
            case max_road_class
                when 'primary'   then 250.0
                when 'secondary' then 150.0
                when 'tertiary'  then 75.0
                else                  50.0
            end

        when traffic_control = 'mini_roundabout' then
            case max_road_class
                when 'primary'   then 250.0
                when 'secondary' then 150.0
                when 'tertiary'  then 75.0
                else                  50.0
            end

        when traffic_control = 'give_way' then
            case max_road_class
                when 'primary'   then 325.0
                when 'secondary' then 175.0
                when 'tertiary'  then 100.0
                else                  75.0
            end

        when traffic_control = 'crossing' then
            case max_road_class
                when 'primary'   then 350.0
                when 'secondary' then 200.0
                when 'tertiary'  then 125.0
                else                  90.0
            end

        else
            case max_road_class
                when 'primary'   then 450.0
                when 'secondary' then 250.0
                when 'tertiary'  then 150.0
                else                  100.0
            end
    end as intersection_cost

from nodes_with_control

{% endmacro %}
