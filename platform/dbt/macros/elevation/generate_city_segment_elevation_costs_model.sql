{#
    Directional elevation cost per segment from the endpoint elevation delta.
    Quadratic in grade (steep costs disproportionately more than shallow);
    separate up/down coefficients (climbing penalized more than descending).

    grade_forward = (end_elev - start_elev) / length_m. Backward is the mirror;
    grade² is sign-independent, so only the coefficient differs by direction.

    A NULL elevation at either endpoint (coverage gap) -> NULL grade -> cost
    coalesces to 0 (treated as flat). start/end elevation and grade are carried
    through for tuning visibility.

    Coefficients are dbt vars so you can tune without editing the macro:
      elevation_uphill_coeff   (default 150)
      elevation_downhill_coeff (default 60)

    Source: stg_<city>_bike_segments, <city>_node_elevations
    Grain: one row per segment_id.
#}
{% macro generate_city_segment_elevation_costs_model(city) %}

{% set uphill_coeff   = var('elevation_uphill_coeff', 150) %}
{% set downhill_coeff = var('elevation_downhill_coeff', 60) %}

with segments as (
    select
        s.segment_id,
        s.start_node_id,
        s.end_node_id,
        s.length_m,
        ns.elevation_m as start_elevation_m,
        ne.elevation_m as end_elevation_m
    from {{ ref('stg_' ~ city ~ '_bike_segments') }} s
    left join {{ ref('stg_' ~ city ~ '_node_elevations') }} ns on ns.node_id = s.start_node_id
    left join {{ ref('stg_' ~ city ~ '_node_elevations') }} ne on ne.node_id = s.end_node_id
),
graded as (
    select
        *,
        (end_elevation_m - start_elevation_m) as rise_forward_m,
        (end_elevation_m - start_elevation_m) / nullif(length_m, 0) as grade_forward
    from segments
)
select
    segment_id,
    start_node_id,
    end_node_id,
    start_elevation_m,
    end_elevation_m,
    rise_forward_m,
    grade_forward,

    -- Forward: climb when grade_forward > 0. coalesce(...,0): gap -> flat.
    coalesce(
        case when grade_forward > 0
             then {{ uphill_coeff }}   * grade_forward * grade_forward * length_m
             else {{ downhill_coeff }} * grade_forward * grade_forward * length_m
        end, 0
    ) as elevation_cost_forward,

    -- Backward: climb when grade_forward < 0 (the reverse traversal ascends).
    coalesce(
        case when grade_forward < 0
             then {{ uphill_coeff }}   * grade_forward * grade_forward * length_m
             else {{ downhill_coeff }} * grade_forward * grade_forward * length_m
        end, 0
    ) as elevation_cost_backward
from graded

{% endmacro %}
