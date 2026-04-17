-- stg__chicago_traffic_control_nodes.sql
-- Classifies each node in the bike network by its traffic control type,
-- intersection status, and the highest road classification meeting there.
--
-- Produces a per-node safety adjustment (traffic_control_penalty) that is
-- added to the destination edge's safety_cost in bike_safety_weighted_edges.
--
-- Grain: one row per node (osmid).
--
-- Key concepts:
--   - Intersection detection uses graph degree (in + out edges >= 3).
--   - Road classification is the "worst" (highest-traffic) highway type
--     among all edges touching the node, via a numeric rank.
--   - All intersection penalties are non-negative. Every intersection
--     adds cost relative to a mid-segment node (which has penalty 0).
--     Traffic control devices reduce the penalty but never eliminate
--     the inherent risk of an intersection crossing.
--   - The best-case intersection (signal on a primary road) has
--     penalty 0 — as safe as an intersection can be, but no safer
--     than a mid-block segment.
--   - Uncontrolled intersections receive the largest penalties, scaled
--     by the road classification of the most dangerous leg.
--
-- Signal nodes in the same physical intersection are clustered together
-- (e.g., separate signals for each cycleway crossing at one intersection).
-- Only the "primary" node in each cluster carries the penalty; others are
-- zeroed to avoid double-counting.

{{ config(materialized='table') }}

-- =====================================================================
-- Node degree: count of directed edges touching each node
-- =====================================================================
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
-- =====================================================================
-- Max road classification at each node
-- =====================================================================
edge_road_ranks as (
    select
        node_id,
        max(road_rank) as max_road_rank
    from (
        select u as node_id, highway, {{ road_rank('highway') }} as road_rank
        from {{ ref('chicago_bike_network_edges') }}
        union all
        select v as node_id, highway, {{ road_rank('highway') }} as road_rank
        from {{ ref('chicago_bike_network_edges') }}
    ) t
    group by node_id
),
-- =====================================================================
-- Cluster nearby traffic control nodes into logical intersections.
-- Nodes within 30m of each other are treated as the same intersection.
-- =====================================================================
control_clusters as (
    select
        osmid,
        highway as traffic_control,
        st_clusterdbscan(geom, eps := 30, minpoints := 1) over () as cluster_id
    from {{ source('raw_data', 'osmnx_chicago_bike_network_nodes') }}
    where valid_to is null
      and highway in ('traffic_signals', 'crossing', 'stop', 'give_way', 'mini_roundabout')
),
-- =====================================================================
-- Pick one primary node per cluster. The node with the strongest control
-- type wins; osmid is a deterministic tiebreaker.
-- =====================================================================
control_primary as (
    select
        osmid,
        cluster_id,
        row_number() over (
            partition by cluster_id
            order by
                case traffic_control
                    when 'traffic_signals'  then 1
                    when 'mini_roundabout'  then 2
                    when 'stop'             then 3
                    when 'give_way'         then 4
                    when 'crossing'         then 5
                    else                         6
                end,
                osmid
        ) = 1 as is_primary
    from control_clusters
),
-- =====================================================================
-- Node classification
-- =====================================================================
nodes as (
    select
        n.osmid,
        n.highway                               as traffic_control,
        nd.degree,
        nd.degree >= 3                          as is_intersection,
        err.max_road_rank,
        case err.max_road_rank
            when 4 then 'primary'
            when 3 then 'secondary'
            when 2 then 'tertiary'
            else        'minor'
        end                                     as max_road_class,
        coalesce(cp.is_primary, true)           as is_primary
    from {{ source('raw_data', 'osmnx_chicago_bike_network_nodes') }} as n
    inner join node_degree nd
        on nd.node_id = n.osmid
    inner join edge_road_ranks err
        on err.node_id = n.osmid
    left join control_primary cp
        on cp.osmid = n.osmid
    where n.valid_to is null
),
weighting as (
    select
        osmid,
        traffic_control,
        degree,
        is_intersection,
        max_road_rank,
        max_road_class,
        is_primary,
        case
            -- Non-primary nodes in a cluster: penalty already applied at primary
            when not is_primary then 0.0
            -- ── Signalized intersections: safest intersection type ──
            when traffic_control = 'traffic_signals' then
                case max_road_class
                    when 'primary'   then  0.0
                    when 'secondary' then  2.0
                    when 'tertiary'  then  5.0
                    else                   7.0
                end
            -- ── Marked crossings (type unknown): some visibility benefit ──
            when traffic_control = 'crossing' then
                case max_road_class
                    when 'primary'   then  7.0
                    when 'secondary' then  7.0
                    when 'tertiary'  then  8.0
                    else                   9.0
                end
            -- ── Stop signs: slows cross-traffic ──
            when traffic_control = 'stop' then
                case max_road_class
                    when 'primary'   then  7.0
                    when 'secondary' then  7.0
                    when 'tertiary'  then  8.0
                    else                   9.0
                end
            -- ── Mini roundabouts: traffic calming ──
            when traffic_control = 'mini_roundabout' then
                case max_road_class
                    when 'primary'   then  7.0
                    when 'secondary' then  7.0
                    when 'tertiary'  then  7.0
                    else                   8.0
                end
            -- ── Yield / give way: minimal protection ──
            when traffic_control = 'give_way' then 10.0
            -- ── Uncontrolled intersection: highest risk, scaled by road class ──
            when traffic_control is null and is_intersection then
                case max_road_class
                    when 'primary'   then 25.0
                    when 'secondary' then 20.0
                    when 'tertiary'  then 15.0
                    else                  12.0
                end
            -- ── Mid-segment node or non-intersection: no adjustment ──
            else 0.0
        end as traffic_control_penalty
    from nodes
)

select * from weighting
