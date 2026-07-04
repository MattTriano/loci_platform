{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('nyc', time_zone = 'America/New_York') }}
