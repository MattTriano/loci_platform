{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('denver', time_zone = 'America/Chicago') }}
