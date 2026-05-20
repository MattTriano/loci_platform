{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('madison', time_zone = 'America/Chicago') }}
