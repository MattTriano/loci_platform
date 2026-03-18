-- bike_commuter__crash_hotspots.sql
-- Bike crash data aggregated for heatmap display.
--
-- Two output modes in one model:
--   1. Individual crash points with a severity score (for zoomed-in view)
--   2. Grid-cell aggregation (for zoomed-out heatmap view)
--
-- For the grid aggregation we use a simple lat/lon grid rather than H3
-- to avoid the PostGIS H3 extension dependency. Each cell is roughly
-- 200m x 200m (~0.002 degrees at Chicago's latitude).
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS ix_{{ this.name }}_geom ON {{ this }} USING GIST (geom)"
    ]
) }}

with crashes_with_severity as (
    select
        crash_record_id,
        geom,
        crash_date,
        local_crash_date,
        local_crash_time,
        local_crash_day_of_week,
        first_crash_type,
        most_severe_injury,
        hit_and_run_i,
        dooring_i,
        weather_condition,
        lighting_condition,
        street_name,
        street_direction,
        prim_contributory_cause,
        injuries_total,
        injuries_fatal,
        injuries_incapacitating,
        case
            when injuries_fatal > 0 then 5
            when injuries_incapacitating > 0 then 4
            when injuries_non_incapacitating > 0 then 3
            when injuries_reported_not_evident > 0 then 2
            else 1
        end as severity_score,
        extract(year from crash_date) as crash_year
    from {{ ref('stg__bike_involved_crashes') }}
)
-- ,grid_agg as (
--     select
--         -- Snap to grid: round lat/lon to nearest 0.002 degrees
--         round(ST_Y(geom)::numeric / 0.002) * 0.002 as grid_lat,
--         round(ST_X(geom)::numeric / 0.002) * 0.002 as grid_lon,
--         count(*) as crash_count,
--         avg(severity_score) as avg_severity,
--         max(severity_score) as max_severity,
--         sum(case when dooring_i = 'Y' then 1 else 0 end) as dooring_count,
--         sum(case when hit_and_run_i = 'Y' then 1 else 0 end) as hit_and_run_count,
--         sum(injuries_fatal) as total_fatal,
--         sum(injuries_incapacitating) as total_incapacitating,
--         min(crash_date) as earliest_crash,
--         max(crash_date) as latest_crash
--     from crashes_with_severity
--     group by grid_lat, grid_lon
-- )

-- Output the individual crash points (the app can aggregate client-side
-- at different zoom levels, or use the grid_agg CTE for a pre-computed
-- heatmap layer).
--
-- To use the grid aggregation instead, change the final select to:
--   select *, ST_SetSRID(ST_MakePoint(grid_lon, grid_lat), 4326) as geom
--   from grid_agg
select * from crashes_with_severity