-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_way_nodes.sql
{{ config(materialized='table') }}

{{ generate_stg_city_way_nodes_model('chicago') }}
