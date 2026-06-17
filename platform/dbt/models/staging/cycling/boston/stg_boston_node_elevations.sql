{{ config(materialized='table') }}

{{ generate_stg_city_node_elevations_model('boston') }}
