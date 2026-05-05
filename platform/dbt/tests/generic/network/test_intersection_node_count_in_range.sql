-- tests/generic/test_intersection_node_count_in_range.sql
{% test intersection_node_count_in_range(model, way_nodes_model, min_ratio=0.05, max_ratio=0.9) %}
-- Intersection nodes should be a meaningful fraction of all nodes,
-- but not most of them. Catches a future bug where the
-- intersection-detection logic produces 0 or ~all nodes.
with stats as (
    select
        (select count(*) from {{ model }}) as intersections,
        (select count(distinct node_id) from {{ way_nodes_model }}) as total_nodes
)
select *
from stats
where
    intersections::float / total_nodes < {{ min_ratio }}
    or intersections::float / total_nodes > {{ max_ratio }}
{% endtest %}