{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('portland', time_zone = 'America/Los_Angeles') }}
