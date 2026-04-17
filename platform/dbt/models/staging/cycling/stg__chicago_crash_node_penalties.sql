-- stg__chicago_crash_node_penalties.sql
-- Attaches bike crashes to intersection nodes, clustering nearby
-- intersection nodes into single logical intersections first.
--
-- OSM sometimes splits a single physical intersection into multiple
-- nodes when separately-drawn road and cycletrack geometries meet
-- (e.g., Belmont Ave road + Belmont Bikeway at Kimball are 7m apart
-- but the same intersection). Without clustering, crashes at such
-- intersections split across the node pair and each node gets a
-- penalty — a route transiting both nodes pays double.
--
-- This model:
--   1. Identifies intersection nodes (degree >= 3 in the bike graph).
--   2. Clusters intersection nodes within 30m of each other (DBSCAN).
--   3. Attaches each crash to the nearest intersection node within 25m.
--   4. Aggregates crash severity per CLUSTER.
--   5. Emits penalty on only the "primary" node (smallest osmid) in
--      each cluster — matching the convention used by
--      stg__chicago_traffic_control_nodes.
--
-- Known weakness: a route that traverses a non-primary node in a
-- cluster without traversing the primary won't pay the penalty. This
-- is the same limitation as traffic_control_nodes, and bounded — the
-- router will typically transit the primary node unless topology
-- specifically forces otherwise.
--
-- Grain: one row per primary intersection node (per cluster) that has
-- any attributed crashes.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} (osmid)"
    ]
) }}

{% set crash_decay_lambda = 0.4 %}
{% set node_crash_radius_m = 25 %}
{% set node_crash_radius_degrees = 0.00025 %}
{% set cluster_radius_m = 30 %}

with node_degree as (
    select
        node_id,
        count(*) as degree
    from (
        select u as node_id from {{ ref('chicago_bike_network_edges') }}
        union all
        select v as node_id from {{ ref('chicago_bike_network_edges') }}
    ) t
    group by node_id
),
intersection_nodes as (
    select
        n.osmid,
        n.geom
    from {{ source('raw_data', 'osmnx_chicago_bike_network_nodes') }} n
    inner join node_degree nd on nd.node_id = n.osmid
    where n.valid_to is null
      and nd.degree >= 3
),
-- =====================================================================
-- Cluster intersection nodes within 30m into single logical
-- intersections. This is the key fix: a physical intersection split
-- across multiple OSM nodes (road + cycletrack) gets unified.
-- =====================================================================
clustered as (
    select
        osmid,
        geom,
        st_clusterdbscan(geom, eps := {{ cluster_radius_m }}, minpoints := 1)
            over () as cluster_id
    from intersection_nodes
),
-- =====================================================================
-- Pick a deterministic primary node for each cluster.
-- =====================================================================
cluster_primary as (
    select
        cluster_id,
        min(osmid) as primary_osmid
    from clustered
    group by cluster_id
),
-- =====================================================================
-- Attach each crash to its nearest intersection node (any in cluster).
-- =====================================================================
crash_to_node as (
    select distinct on (c.crash_record_id)
        c.crash_record_id,
        c.severity_score,
        c.crash_date,
        cl.cluster_id
    from {{ ref('chicago_bike_crash_hotspots') }} c
    inner join clustered cl
        on cl.geom && ST_Expand(c.geom, {{ node_crash_radius_degrees }})
        and ST_DWithin(c.geom, cl.geom, {{ node_crash_radius_degrees }})
    order by c.crash_record_id, ST_Distance(c.geom, cl.geom)
),
-- =====================================================================
-- Aggregate by cluster, emit on primary node.
-- =====================================================================
cluster_scores as (
    select
        cluster_id,
        count(*) as crash_count,
        sum(
            power(severity_score, 2) * exp(
                -{{ crash_decay_lambda }}
                * extract(epoch from (current_date - crash_date))
                / (365.25 * 86400)
            )
        ) as crash_score
    from crash_to_node
    group by cluster_id
)

select
    cp.primary_osmid as osmid,
    cs.crash_count   as node_crash_count,
    cs.crash_score   as node_crash_score
from cluster_scores cs
join cluster_primary cp using (cluster_id)
