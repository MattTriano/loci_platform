{{ config(materialized='table') }}

{{ generate_stg_city_bike_ways_model('detroit', include_all_sidewalks=true) }}
