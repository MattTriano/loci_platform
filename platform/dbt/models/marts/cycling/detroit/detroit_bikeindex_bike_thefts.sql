{{ config(materialized='table') }}

{{ generate_city_bikeindex_bike_thefts_model('detroit', time_zone = 'America/Detroit') }}
