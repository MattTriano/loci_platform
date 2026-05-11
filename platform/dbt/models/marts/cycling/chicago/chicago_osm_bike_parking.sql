-- loci_platform/platform/dbt/models/marts/cycling/chicago/chicago_bike_parking.sql
{{ config(materialized='table') }}

{{ generate_city_osm_bike_parking_model('chicago') }}
