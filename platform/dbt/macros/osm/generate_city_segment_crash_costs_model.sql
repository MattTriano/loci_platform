{#
    Per-segment crash-derived stress cost.
    Built only for cities with a bike-crash data source.

    Sources: stg_<city>_bike_segments + <city>_bike_crash_hotspots
    Grain: one row per segment that has at least one nearby crash.

    Crash deduplication: each crash is assigned to exactly one segment
    via DISTINCT ON, prioritizing nearest distance, breaking ties by
    most-dangerous highway class.

    Crash score: severity-squared, exponentially decayed by crash age.
    Crash cost: crash_score * crash_weight.

    Note: the previous formulation computed crash_score / length_m and
    then multiplied back by length_m * crash_weight. That algebraically
    equals crash_score * crash_weight, so the per-meter normalization
    is dropped here. A future refactor may reintroduce nonlinear scaling
    in crash density to better capture short-segment crash clusters vs.
    long-segment dispersed crashes.

    Parameters:
      city: city name.
      crash_weight: scalar multiplier applied to crash_score.
      crash_decay_lambda: exponential decay rate per year of crash age.
      crash_buffer_degrees: spatial buffer (degrees) for matching
        crashes to segments.
#}
{% macro generate_city_segment_crash_costs_model(
    city,
    crash_weight=24.0,
    crash_decay_lambda=0.4,
    crash_buffer_degrees=0.0002
) %}

with segments as (
    select * from {{ ref('stg_' ~ city ~ '_bike_segments') }}
),
crashes as (
    select * from {{ ref(city ~ '_bike_crash_hotspots') }}
),

-- Crash deduplication: assign each crash to exactly one segment.
-- Priority: nearest by distance, then most dangerous highway class.
crash_nearest as (
    select distinct on (c.crash_record_id)
        c.crash_record_id,
        c.severity_score,
        c.crash_date,
        s.segment_id
    from crashes c
    inner join segments s
        on s.geom && ST_Expand(c.geom, {{ crash_buffer_degrees }})
        and ST_DWithin(c.geom, s.geom, {{ crash_buffer_degrees }})
    order by c.crash_record_id,
        ST_Distance(c.geom, s.geom),
        case
            when s.highway in ('primary', 'primary_link')
                or s.highway like '%primary%'         then 1
            when s.highway in ('secondary', 'secondary_link')
                or s.highway like '%secondary%'       then 2
            when s.highway in ('tertiary', 'tertiary_link')
                or s.highway like '%tertiary%'        then 3
            when s.highway = 'unclassified'           then 4
            when s.highway = 'residential'
                or s.highway like '%residential%'     then 5
            when s.highway = 'service'                then 6
            else 7
        end
),
crash_scores as (
    select
        cn.segment_id,
        count(*) as crash_count,
        sum(
            power(cn.severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - cn.crash_date))
                / (365.25 * 86400)
            )
        ) as crash_score
    from crash_nearest cn
    group by cn.segment_id
)

select
    segment_id,
    crash_count,
    crash_score,
    crash_score * {{ crash_weight }} as crash_cost
from crash_scores

{% endmacro %}
