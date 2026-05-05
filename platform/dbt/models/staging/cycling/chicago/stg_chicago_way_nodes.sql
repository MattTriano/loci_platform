-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_way_nodes.sql
{{ config(materialized='view') }}

select
    osm_id as way_id,
    ordinality as position,
    node_id,
    array_length(node_ids, 1) as way_length
from {{ ref('stg_chicago_bike_ways') }},
     unnest(node_ids) with ordinality as t(node_id, ordinality)
