{#
    Final stress-weighted bike segments. Joins per-segment cost
    components and sums them into a single stress_cost per segment.

    Cost formula:
      stress_cost = physical_cost + crash_cost + intersection_cost

    Each component model owns its own multiplications. This model just
    joins and adds.

    Sources:
      - <city>_segment_costs (always)
      - <city>_segment_crash_costs (when include_crashes=true)
      - <city>_intersection_costs (always; joined on end_node_id)

    Grain: one row per segment (segment_id). Direction-symmetric: forward
    and backward traversal share the same stress_cost. The graph
    exporter handles direction.

    Parameters:
      city: city name.
      include_crashes: when true, joins crash costs. Set to false for
        cities without a bike-crash data source.
#}
{% macro generate_city_bike_stress_weighted_segments_model(city, include_crashes=true) %}

with segment_costs as (
    select * from {{ ref(city ~ '_segment_costs') }}
),
{% if include_crashes %}
crash_costs as (
    select * from {{ ref(city ~ '_segment_crash_costs') }}
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
    sc.maxspeed,

    -- Raw cycleway tags
    sc.cycleway,
    sc.cycleway_left,
    sc.cycleway_right,

    -- Physical factors (preserved for tuning)
    sc.speed_factor,
    sc.road_type_factor,
    sc.infrastructure_factor,
    sc.tunnel_factor,
    sc.surface_factor,
    sc.lighting_factor,

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
    coalesce(ic.intersection_cost, 0) as intersection_cost,

    -- Final composition
    sc.physical_cost
        {% if include_crashes %}+ coalesce(cc.crash_cost, 0){% endif %}
        + coalesce(ic.intersection_cost, 0)
        as stress_cost

from segment_costs sc
{% if include_crashes %}
left join crash_costs cc on cc.segment_id = sc.segment_id
{% endif %}
left join intersection_costs ic on ic.osmid = sc.end_node_id

{% endmacro %}
