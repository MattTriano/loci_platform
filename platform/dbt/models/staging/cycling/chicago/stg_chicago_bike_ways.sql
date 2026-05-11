-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_bike_ways.sql
{{ config(materialized='table') }}

{{ generate_stg_city_bike_ways_model('chicago', include_all_sidewalks=false) }}
