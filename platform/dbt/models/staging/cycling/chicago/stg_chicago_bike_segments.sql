-- loci_platform/platform/dbt/models/staging/cycling/chicago/stg_chicago_bike_segments.sql
-- Bike network segments, one row per intersection-to-intersection
-- portion of a way, with bike infrastructure classification and
-- physical conditions joined in.
--
-- Source: stg_chicago_way_segments + stg_chicago_osm_bike_infra
-- Grain: one row per segment (way_id, start_position, end_position).
--
-- Direction handling: each segment carries a `direction` column
-- ('forward', 'backward', 'bidirectional') indicating which directions
-- of travel are legal. The graph exporter expands these into one or
-- two directed edges per segment.

{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} (way_id, start_position, end_position)",
        "CREATE INDEX ON {{ this }} (start_node_id)",
        "CREATE INDEX ON {{ this }} (end_node_id)",
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}


{{ generate_stg_city_bike_segments_model('chicago') }}
