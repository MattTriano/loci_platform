-- models/staging/cycling/chicago/stg_chicago_osm_bike_infra.sql
-- Classifies bike network segments into infrastructure categories
-- using the full set of cycleway/bicycle tags.
--
-- Source: stg_chicago_way_segments
-- Grain: one row per segment with bike infrastructure present.
--
-- Infrastructure taxonomy:
--   infra_category: 'separated', 'on_road', 'shared', 'path'
--   infra_type:     'protected_lane', 'track', 'buffered_lane',
--                   'bike_lane', 'sharrow', 'bicycle_road',
--                   'share_busway', 'shared_path', 'designated_path'
--
-- The classification logic resolves left/right/both cycleway tags into
-- a single per-segment classification. When left and right differ, we
-- use the higher-quality side (separated > on_road > shared).

{{ config(materialized='table') }}

{{ generate_stg_city_osm_bike_infra_model('chicago', include_all_sidewalks=false) }}
