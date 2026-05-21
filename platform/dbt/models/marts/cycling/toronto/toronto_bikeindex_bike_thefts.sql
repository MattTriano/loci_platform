{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('toronto', time_zone = 'America/Chicago') }}
