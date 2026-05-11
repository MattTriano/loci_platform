-- tests/generic/test_segments_cover_full_ways.sql
{% test segments_cover_full_ways(model, way_nodes_model, intersection_nodes_model) %}
-- For each way, the number of segments should equal (number of
-- intersection nodes in that way) - 1. If a way's segments don't
-- collectively cover its full intersection-to-intersection structure,
-- this catches it.
with way_intersection_positions as (
    select
        wn.way_id,
        wn.position
    from {{ way_nodes_model }} wn
    inner join {{ intersection_nodes_model }} i on wn.node_id = i.node_id
),
expected_segment_count as (
    select way_id, count(*) - 1 as expected
    from way_intersection_positions
    group by way_id
    having count(*) >= 2
),
actual_segment_count as (
    select way_id, count(*) as actual
    from {{ model }}
    group by way_id
)
select
    e.way_id,
    e.expected,
    coalesce(a.actual, 0) as actual
from expected_segment_count as e
left join actual_segment_count as a
    on e.way_id = a.way_id
where coalesce(a.actual, 0) != e.expected
{% endtest %}