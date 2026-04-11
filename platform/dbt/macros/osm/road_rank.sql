-- macros/road_rank.sql
-- Maps an OSM highway type to a numeric rank for comparison.
-- Higher rank = more dangerous / higher-traffic road.
--
-- Used by int_node_traffic_control to determine the worst road
-- classification meeting at each intersection node.

{% macro road_rank(highway_column) %}
    case
        when {{ highway_column }} in ('primary', 'primary_link')
            or {{ highway_column }} like '%primary%'         then 4
        when {{ highway_column }} in ('secondary', 'secondary_link')
            or {{ highway_column }} like '%secondary%'       then 3
        when {{ highway_column }} in ('tertiary', 'tertiary_link')
            or {{ highway_column }} like '%tertiary%'        then 2
        else                                                      1
    end
{% endmacro %}
