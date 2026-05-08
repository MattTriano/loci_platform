{% macro generate_stg_city_intersection_nodes_model(city) %}

with node_way_counts as (
    select
        node_id,
        count(distinct way_id) as way_count
    from {{ ref('stg_' ~ city ~ '_way_nodes') }}
    group by node_id
),
endpoints as (
    select node_id
    from {{ ref('stg_' ~ city ~ '_way_nodes') }}
    where position = 1 or position = way_length
)
select distinct node_id
from (
    select node_id from node_way_counts where way_count >= 2
    union
    select node_id from endpoints
) t

{% endmacro %}
