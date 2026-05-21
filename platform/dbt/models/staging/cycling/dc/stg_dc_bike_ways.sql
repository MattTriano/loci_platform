{{ config(materialized='table') }}

{{ generate_stg_city_bike_ways_model('dc', include_all_sidewalks=false) }}
