{{ config(materialized='table') }}

{{ generate_stg_city_intersection_nodes_model('boston') }}
