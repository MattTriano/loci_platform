{{ config(materialized='table') }}

{{ generate_stg_city_way_segments_model('detroit') }}
