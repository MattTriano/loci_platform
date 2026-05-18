{{ config(materialized='table') }}

{{ generate_stg_city_way_nodes_model('sf') }}
