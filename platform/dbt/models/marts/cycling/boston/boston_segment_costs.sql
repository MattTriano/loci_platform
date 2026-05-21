{{ config(materialized='table') }}

{{ generate_city_segment_costs_model('boston') }}
