{{ config(materialized='table') }}

{{ generate_stg_city_intersection_costs_model('madison') }}
