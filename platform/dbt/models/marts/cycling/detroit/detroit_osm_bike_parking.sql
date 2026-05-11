-- loci_platform/platform/dbt/models/marts/cycling/detroit/detroit_osm_bike_parking.sql
{{ config(materialized='table') }}

{{ generate_city_osm_bike_parking_model('detroit') }}
