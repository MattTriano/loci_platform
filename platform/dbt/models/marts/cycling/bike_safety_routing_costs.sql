-- bike_safety_routing_costs.sql
-- Computes a scalar routing cost per edge for safety-weighted bike routing.
-- Consumes bike_safety_weighted_edges and produces the minimal column set
-- needed to build an in-memory graph for shortest-path queries.
--
-- Cost function:
--   cost = length_m * (1 + alpha * crash_score_per_meter + beta * tier_penalty)
--   tier_penalty = (quality_tier - 1) / 9.0   (maps 1–10 onto 0.0–1.0)
--
-- Reverse cost:
--   For bidirectional edges: reverse_cost = cost
--   For one-way edges: reverse_cost = -1  (signals "no traversal" to the router)

{{ config(materialized='table') }}

{% set alpha = 100 %}
{% set beta = 0.5 %}

select
    u,
    v,
    key,
    length_m,
    quality_tier,
    crash_score_per_meter,
    length_m * (
        1.0
        + {{ alpha }} * crash_score_per_meter
        + {{ beta }} * (quality_tier - 1) / 9.0
    ) as cost,
    case
        when oneway = true then -1.0
        else length_m * (
            1.0
            + {{ alpha }} * crash_score_per_meter
            + {{ beta }} * (quality_tier - 1) / 9.0
        )
    end as reverse_cost,
    geom
from {{ ref('bike_safety_weighted_edges') }}
where length_m > 0
