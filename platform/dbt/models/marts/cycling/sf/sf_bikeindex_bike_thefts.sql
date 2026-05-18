{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('sf', time_zone = 'America/Los_Angeles') }}
