-- loci_platform/platform/dbt/models/intermediate/cycling/chicago/stg_chicago_way_segments.sql
{{ config(materialized='table') }}

{{ generate_stg_city_way_segments_model('chicago') }}
