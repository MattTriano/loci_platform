{% macro generate_stg_city_way_nodes_model(city) %}

select
    osm_id as way_id,
    ordinality as position,
    node_id,
    array_length(node_ids, 1) as way_length
from {{ ref('stg_' ~ city ~ '_bike_ways') }},
     unnest(node_ids) with ordinality as t(node_id, ordinality)

{% endmacro %}