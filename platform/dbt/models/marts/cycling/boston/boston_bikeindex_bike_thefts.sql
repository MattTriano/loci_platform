{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('boston', time_zone = 'America/New_York') }}
