{#
    Identifies "logical intersections" in a city's bike network and assigns each
    one a stress penalty representing the cost of a cyclist traversing it.

    A "logical intersection" is a node where at least two distinct OSM ways meet.
    Unlike the previous version, we include cycleway×cycleway and other
    car-free junctions (with a small but nonzero cost), so the routing graph
    has a complete intersection-cost story for every junction.

    Penalty design:
      - Each way meeting the node is classified into one of seven stress
        classes: motorway, primary, secondary, tertiary, local, service, quiet.
      - The base cost looks up (max_class, second_max_class) — but only as a
        primary key on max_class with a handful of explicit overrides for
        the cases where the second-class meaningfully changes the risk
        (e.g. local×service is cheaper than local×local).
      - Traffic control applies as a multiplier on the base (signals reduce
        cost most, no control highest).
      - Direction-symmetric: the penalty is a property of the node itself.
        The graph exporter joins these costs onto segments at *both* endpoints
        and the per-direction cost is selected when each WriteEdge is built.

    Joined into <city>_bike_stress_weighted_segments on both
    start_node_id and end_node_id.

#}
{% macro generate_stg_city_intersection_costs_model(city) %}

with ways_at_node as (
    -- Distinct (node, way) pairs with the way's classified stress class.
    -- A self-loop counts a way only once at each of its nodes.
    select distinct
        wn.node_id,
        wn.way_id,
        case
            when w.highway in ('motorway', 'motorway_link', 'trunk',
                               'trunk_link')                          then 'motorway'
            when w.highway in ('primary', 'primary_link')             then 'primary'
            when w.highway in ('secondary', 'secondary_link')         then 'secondary'
            when w.highway in ('tertiary', 'tertiary_link')           then 'tertiary'
            when w.highway in ('residential', 'unclassified',
                               'living_street', 'busway')             then 'local'
            when w.highway = 'service'                                 then 'service'
            when w.highway in ('cycleway', 'path', 'footway',
                               'pedestrian', 'bridleway', 'steps')   then 'quiet'
            else 'local'
        end as way_class
    from {{ ref('stg_' ~ city ~ '_way_nodes') }} as wn
    inner join {{ ref('stg_' ~ city ~ '_bike_ways') }} as w
        on w.osm_id = wn.way_id
),

-- Rank each way class numerically so we can pick the top two per node.
ranked as (
    select
        node_id,
        way_class,
        case way_class
            when 'motorway'  then 7
            when 'primary'   then 6
            when 'secondary' then 5
            when 'tertiary'  then 4
            when 'local'     then 3
            when 'service'   then 2
            when 'quiet'     then 1
        end as class_rank
    from ways_at_node
),

-- Per-node: highest class, second-highest class, and total distinct way count.
-- We need >= 2 distinct ways for the node to be an intersection at all.
per_node as (
    select
        node_id,
        count(*) as way_count,
        (array_agg(way_class order by class_rank desc))[1] as max_class,
        (array_agg(way_class order by class_rank desc))[2] as second_max_class
    from ranked
    group by node_id
),

logical_intersections as (
    select *
    from per_node
    where way_count >= 2
),

-- Attach the traffic control tag from the raw OSM nodes source.
-- Nodes missing from the source get NULL -> treated as uncontrolled.
nodes_with_control as (
    select
        li.node_id as osmid,
        n.tags->>'highway' as traffic_control,
        li.max_class,
        li.second_max_class,
        li.way_count
    from logical_intersections as li
    left join {{ source('osm', city ~ '_osm_bike_network_nodes') }} as n
        on n.osm_type = 'node'
        and n.osm_id = li.node_id
        and n.valid_to is null
),

-- Base cost by (max_class, second_max_class). For most max_class values
-- the second class doesn't change the risk story much (a primary
-- crossing is a primary crossing regardless of whether the other way
-- is a residential or a quiet path). The exceptions are at the lower
-- end, where local×service and service×quiet are meaningfully calmer.
with_base as (
    select
        osmid,
        traffic_control,
        max_class,
        second_max_class,
        way_count,
        case max_class
            when 'motorway'  then 600.0
            when 'primary'   then 450.0
            when 'secondary' then 250.0
            when 'tertiary'  then 150.0
            when 'local' then
                case second_max_class
                    when 'service' then 40.0
                    when 'quiet'   then 50.0
                    else                60.0
                end
            when 'service' then
                case second_max_class
                    when 'service' then 10.0
                    when 'quiet'   then 15.0
                    else                20.0
                end
            when 'quiet'     then 5.0
            else                  60.0
        end as base_cost
    from nodes_with_control
)

select
    osmid,
    traffic_control,
    max_class,
    second_max_class,
    way_count,
    base_cost,
    -- Final cost = base * traffic_control_multiplier. Multipliers below
    -- are calibrated so a controlled crossing is significantly less
    -- stressful than an uncontrolled one of the same class.
    base_cost * case traffic_control
        when 'traffic_signals'  then 0.45
        when 'stop'             then 0.55
        when 'mini_roundabout'  then 0.55
        when 'give_way'         then 0.75
        when 'crossing'         then 0.80
        else                         1.00
    end as intersection_cost

from with_base

{% endmacro %}
