-- bike_commuter__theft_hotspots.sql
-- Bike theft data from two sources, unified for heatmap display.
--
-- Combines:
--   1. Chicago PD crime reports (IUCR 0917/0918)
--   2. Bike Index stolen bike reports (when available)
--
-- These sources have different strengths:
--   - CPD data: official, consistent format, but underreported
--   - Bike Index: lower reporting barrier, richer detail, but biased
--     toward more expensive bikes and more engaged cyclists
--
-- For the app, we present them as a single theft density layer.


with cpd_thefts as (
    select
        'cpd' as source,
        id::text as source_id,
        date as theft_date,
        extract(year from date)::int as theft_year,
        extract(hour from date at time zone 'America/Chicago')::int as theft_hour,
        location_description::text,
        null::text as bike_description,
        null::text as theft_description,
        null::text as locking_description,
        latitude,
        longitude,
        location
    from {{ ref('stg__crimes__bike_theft') }}
),
bike_index_thefts as (
    select
        'bike_index' as source,
        id::text as source_id,
        date_stolen as theft_date,
        extract(year from date_stolen)::int as theft_year,
        extract(hour from date_stolen at time zone 'America/Chicago')::int as theft_hour,
        null::text as location_description,
        title::text as bike_description,
        theft_description::text,
        locking_description::text,
        latitude,
        longitude,
        geom as location
    from {{ ref('stg__bikeindex_chicago_stolen_bikes') }}
),
all_thefts as (
    select * from cpd_thefts
    union all
    select * from bike_index_thefts
)

select
    source,
    source_id,
    theft_date,
    theft_year,
    theft_hour,
    location_description,
    bike_description,
    theft_description,
    locking_description,
    latitude,
    longitude,
    location
from all_thefts
