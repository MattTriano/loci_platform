{{ config(materialized='table') }}

{{ generate_city_segment_elevation_costs_model('boston') }}
