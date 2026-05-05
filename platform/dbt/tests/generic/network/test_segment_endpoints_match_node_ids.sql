-- tests/generic/test_segment_endpoints_match_node_ids.sql
{% test segment_endpoints_match_node_ids(model, ways_model) %}
-- Each segment's start/end node IDs should match the node IDs at
-- start_position and end_position in the parent way's node_ids array.
select
    s.way_id,
    s.start_position,
    s.end_position,
    s.start_node_id as segment_start,
    s.end_node_id as segment_end,
    w.node_ids[s.start_position] as way_start,
    w.node_ids[s.end_position] as way_end
from {{ model }} as s
inner join {{ ways_model }} as w
    on s.way_id = w.osm_id
where
    s.start_node_id != w.node_ids[s.start_position]
    or s.end_node_id != w.node_ids[s.end_position]
{% endtest %}