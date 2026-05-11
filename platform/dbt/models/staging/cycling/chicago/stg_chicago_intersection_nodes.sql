-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_intersection_nodes.sql
{{ config(materialized='table') }}

{{ generate_stg_city_intersection_nodes_model('chicago') }}
