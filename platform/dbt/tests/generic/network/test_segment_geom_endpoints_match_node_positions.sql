-- tests/generic/test_segment_geom_endpoints_match_node_positions.sql
{% test segment_geom_endpoints_match_node_positions(model, ways_model, max_drift_meters=5) %}
-- Sanity check on the ST_LineSubstring approximation: the start/end
-- points of each segment's geometry should be close to the way's
-- vertex at the corresponding position.
--
-- "Close" because ST_LineSubstring interpolates by length, not
-- vertex index, so non-uniform shape-point spacing causes some drift.
-- If this fails on a meaningful number of segments, switch to
-- vertex-indexed reconstruction.
select
    s.way_id,
    s.start_position,
    ST_Distance(
        ST_StartPoint(s.geom)::geography,
        ST_PointN(w.geom, s.start_position::integer)::geography
    ) as start_drift_m,
    ST_Distance(
        ST_EndPoint(s.geom)::geography,
        ST_PointN(w.geom, s.end_position::integer)::geography
    ) as end_drift_m
from {{ model }} s
inner join {{ ways_model }} w on s.way_id = w.osm_id
where ST_Distance(ST_StartPoint(s.geom)::geography, ST_PointN(w.geom, s.start_position::integer)::geography) > {{ max_drift_meters }}
   or ST_Distance(ST_EndPoint(s.geom)::geography, ST_PointN(w.geom, s.end_position::integer)::geography) > {{ max_drift_meters }}
{% endtest %}