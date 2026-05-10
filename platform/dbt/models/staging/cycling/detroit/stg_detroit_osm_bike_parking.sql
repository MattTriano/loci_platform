-- loci_platform/platform/dbt/models/staging/cycling/detroit/stg_detroit_osm_bike_parking.sql
{{ config(materialized='table') }}

{{ generate_stg_city_osm_bike_parking_model('detroit') }}
