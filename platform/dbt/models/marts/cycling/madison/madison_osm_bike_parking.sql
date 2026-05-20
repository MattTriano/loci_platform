{{ config(materialized='table') }}

{{ generate_city_osm_bike_parking_model('madison') }}
