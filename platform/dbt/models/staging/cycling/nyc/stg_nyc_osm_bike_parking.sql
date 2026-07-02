{{ config(materialized='table') }}

{{ generate_stg_city_osm_bike_parking_model('nyc') }}
