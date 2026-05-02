-- stg_chicago__bike_routes.sql
-- Staging model for Chicago bike routes from the city data portal.
-- Source: https://data.cityofchicago.org/Transportation/Bike-Routes/hvv9-38ut
--
-- Standardizes column names and maps displayrou values to the common
-- infra_type taxonomy used across all bike infrastructure models.
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX ON {{ this }} USING GIST (geom)",
        "ANALYZE {{ this }}"
    ]
) }}

select
    -- We don't have a natural key, so build one from the segment definition
    md5(
        coalesce(street, '') || '|'
        || coalesce(f_street, '') || '|'
        || coalesce(t_street, '') || '|'
        || coalesce(displayrou, '')
    ) as route_segment_id,

    the_geom as geom,
    street as street_name,
    st_name,
    f_street as from_street,
    t_street as to_street,
    displayrou as display_route_type,
    case displayrou
        when 'Protected Bike Lane'  then 'protected_lane'
        when 'Buffered Bike Lane'   then 'buffered_lane'
        when 'Bike Lane'            then 'bike_lane'
        when 'Marked Shared Lane'   then 'sharrow'
        when 'Neighborhood Greenway' then 'neighborhood_greenway'
        else 'other'
    end as infra_type,
    mi_ctrline::numeric as centerline_miles,
    oneway_dir,
    br_oneway as bike_route_oneway,
    br_ow_dir as bike_route_oneway_dir,
    contraflow,
    'chicago_data_portal' as data_source,
    ingested_at
from {{ source('raw_data', 'chicago_bike_routes') }}
where valid_to is null
