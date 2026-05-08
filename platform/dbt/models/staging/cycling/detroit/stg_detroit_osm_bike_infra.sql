{{ config(materialized='table') }}

{{ generate_stg_city_osm_bike_infra_model('detroit', include_all_sidewalks=true) }}
