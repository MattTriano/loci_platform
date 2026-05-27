{#
    Identifies "logical intersections" in a city's bike network and assigns each
    one a stress penalty representing the cost of a cyclist traversing it.

    A "logical intersection" is a node where at least two distinct OSM ways meet
    AND at least one of those ways carries cars.

    Penalty design:
      - Each car-carrying leg has a base cost determined by its road class
          (primary, secondary, tertiary/busway, unclassified, residential, service).
      - Per node, we take the worst leg's base cost at full weight and add a
          fraction (alpha) of the sum of the remaining car-carrying legs' base
          costs. This captures the worst-stream-dominates intuition while still
          recognizing that more car streams = more stress.
      - A traffic-control multiplier scales the result downward when there is
          signalization or other control reducing the burden on the cyclist.
      - Direction-symmetric: the penalty is a property of the node, not the
          path through it. Turn-specific costs are handled separately in the
          routing algorithm.

    Joined into <city>_bike_stress_weighted_segments on end_node_id.
#}
{% macro generate_stg_city_intersection_costs_model(city) %}

{# Weight applied to non-worst car-carrying legs. #}
{% set alpha = 0.3 %}

-- =====================================================================
-- Each (node, way) pair, with the way's base cost if it carries cars.
-- Non-car-carrying ways get NULL base_cost; they still count toward
-- way_count but contribute nothing to the cost arithmetic.
-- =====================================================================
with ways_at_node as (
    select distinct
        wn.node_id,
        wn.way_id,
        w.highway,
        case
            when w.highway in ('primary', 'primary_link')
                or w.highway like '%primary%'                   then 600.0
            when w.highway in ('secondary', 'secondary_link')
                or w.highway like '%secondary%'                 then 400.0
            when w.highway in ('tertiary', 'tertiary_link', 'busway')
                or w.highway like '%tertiary%'                  then 250.0
            when w.highway = 'unclassified'                     then 220.0
            when w.highway in ('residential')
                or w.highway like '%residential%'               then 70.0
            when w.highway = 'service'                          then 40.0
            else                                                     null
        end as leg_base_cost
    from {{ ref('stg_' ~ city ~ '_way_nodes') }} as wn
    inner join {{ ref('stg_' ~ city ~ '_bike_ways') }} as w
        on w.osm_id = wn.way_id
),
-- =====================================================================
-- Per-node summary: distinct way count, car-carrying leg count, and the
-- worst-leg base cost plus the sum of the remaining car-carrying legs.
-- =====================================================================
node_summary as (
    select
        node_id,
        count(*) as way_count,
        count(leg_base_cost) as car_way_count,
        coalesce(max(leg_base_cost), 0) as worst_leg_base_cost,
        coalesce(sum(leg_base_cost), 0)
            - coalesce(max(leg_base_cost), 0) as other_legs_base_cost_sum
    from ways_at_node
    group by node_id
),
-- =====================================================================
-- Logical intersections: 2+ distinct ways AND >= 1 carries cars.
-- =====================================================================
logical_intersections as (
    select
        node_id,
        way_count,
        car_way_count,
        worst_leg_base_cost,
        other_legs_base_cost_sum
    from node_summary
    where way_count >= 2
      and car_way_count >= 1
),
-- =====================================================================
-- Attach traffic-control tag from the raw OSM nodes source and resolve
-- the control multiplier. Missing nodes get NULL -> uncontrolled.
-- =====================================================================
nodes_with_control as (
    select
        li.node_id as osmid,
        n.tags->>'highway' as traffic_control,
        li.way_count,
        li.car_way_count,
        li.worst_leg_base_cost,
        li.other_legs_base_cost_sum,
        case n.tags->>'highway'
            when 'traffic_signals' then 0.4
            when 'stop'            then 0.55
            when 'mini_roundabout' then 0.55
            when 'give_way'        then 0.7
            when 'crossing'        then 0.8
            else                        1.0
        end as control_multiplier
    from logical_intersections as li
    left join {{ source('osm', city ~ '_osm_bike_network_nodes') }} as n
        on n.osm_type = 'node'
        and n.osm_id = li.node_id
        and n.valid_to is null
)

select
    osmid,
    traffic_control,
    way_count,
    car_way_count,
    worst_leg_base_cost,
    other_legs_base_cost_sum,
    control_multiplier,
    (worst_leg_base_cost + {{ alpha }} * other_legs_base_cost_sum)
        * control_multiplier as intersection_cost

from nodes_with_control

{% endmacro %}
