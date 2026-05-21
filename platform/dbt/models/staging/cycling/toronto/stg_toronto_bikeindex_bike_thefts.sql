{{ config(materialized='table') }}

{{ generate_stg_city_bikeindex_bike_thefts_model('toronto') }}
