{#
    Final stress-weighted bike segments. Joins per-segment cost components
    plus the intersection costs at *both* endpoints.

    Cost composition:
      The exporter assembles per-direction stress when building each
      directed edge:
        forward edge:  physical_cost + crash_cost + intersection_cost_at_end
        backward edge: physical_cost + crash_cost + intersection_cost_at_start

      This model does NOT compose a single `stress_cost` column — there's
      no honest direction-symmetric value to put there. Segment-level cost
      is `physical_cost + crash_cost`; intersection cost is exposed per
      endpoint and selected by the exporter based on traversal direction.

    Sources:
      - <city>_segment_costs (always)
      - <city>_segment_crash_costs (when include_crashes=true)
      - <city>_intersection_costs (always; joined twice, on start and end)

    Grain: one row per segment (segment_id).

    Parameters:
      city: city name.
      include_crashes: when true, joins crash costs. Set to false for
        cities without a bike-crash data source.
#}
{% macro generate_city_bike_stress_weighted_segments_model(city, include_crashes=true, include_elevation=false) %}

with segment_costs as (
    select * from {{ ref(city ~ '_segment_costs') }}
),
{% if include_crashes %}
crash_costs as (
    select * from {{ ref(city ~ '_segment_crash_costs') }}
),
{% endif %}
{% if include_elevation %}
elevation_costs as (
    select * from {{ ref(city ~ '_segment_elevation_costs') }}
),
{% endif %}
intersection_costs as (
    select * from {{ ref(city ~ '_intersection_costs') }}
)

select
    -- Identity
    sc.segment_id,
    sc.way_id,
    sc.start_position,
    sc.end_position,

    -- Topology
    sc.start_node_id,
    sc.end_node_id,
    sc.direction,

    -- Geometry and length
    sc.geom,
    sc.length_m,

    -- Identification
    sc.name,
    sc.highway,

    -- Bike infrastructure
    sc.infra_category,
    sc.infra_type,
    sc.has_buffer,

    -- Physical conditions
    sc.surface,
    sc.lit,
    sc.bridge,
    sc.tunnel,
    sc.layer,
    sc.covered,
    sc.maxspeed,

    -- Raw cycleway tags
    sc.cycleway,
    sc.cycleway_left,
    sc.cycleway_right,

    -- Cost classification (preserved for tuning)
    sc.highway_class,
    sc.infra_tier,
    sc.base_stress_per_meter,
    sc.surface_penalty,
    sc.enclosed_penalty,
    sc.lighting_penalty,

    -- Cost components
    sc.physical_cost,
    {% if include_crashes %}
    coalesce(cc.crash_count, 0) as crash_count,
    coalesce(cc.crash_score, 0) as crash_score,
    coalesce(cc.crash_cost, 0)  as crash_cost,
    {% else %}
    0 as crash_count,
    0 as crash_score,
    0 as crash_cost,
    {% endif %}

    -- Elevation: directional costs selected by the exporter per direction.
    -- start/end elevation and grade carried for tuning visibility.
    {% if include_elevation %}
    ec.start_elevation_m,
    ec.end_elevation_m,
    ec.grade_forward,
    coalesce(ec.elevation_cost_forward, 0)  as elevation_cost_forward,
    coalesce(ec.elevation_cost_backward, 0) as elevation_cost_backward,
    {% else %}
    null::double precision as start_elevation_m,
    null::double precision as end_elevation_m,
    null::double precision as grade_forward,
    0 as elevation_cost_forward,
    0 as elevation_cost_backward,
    {% endif %}

    -- Intersection costs at both endpoints. The exporter selects the
    -- right one per direction. NULL endpoints (nodes that aren't
    -- logical intersections) default to 0.
    coalesce(ic_start.intersection_cost, 0) as intersection_cost_at_start,
    coalesce(ic_end.intersection_cost,   0) as intersection_cost_at_end,

    -- Whether each endpoint is a logical intersection (used by the
    -- graph exporter to populate WriteNode.is_intersection without
    -- re-deriving from degree on the Rust side). True iff the node
    -- appears in <city>_intersection_costs.
    ic_start.osmid is not null as start_is_intersection,
    ic_end.osmid   is not null as end_is_intersection

from segment_costs sc
{% if include_crashes %}
left join crash_costs cc on cc.segment_id = sc.segment_id
{% endif %}
{% if include_elevation %}
left join elevation_costs ec on ec.segment_id = sc.segment_id
{% endif %}
left join intersection_costs ic_start on ic_start.osmid = sc.start_node_id
left join intersection_costs ic_end   on ic_end.osmid   = sc.end_node_id

{% endmacro %}
