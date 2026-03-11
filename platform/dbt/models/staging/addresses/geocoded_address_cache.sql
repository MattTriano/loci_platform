-- Persistent geocoding cache. Unions addresses from all source datasets,
-- deduplicates by normalized address hash, and preserves source-provided
-- lat/lng where available. Rows with NULL lat/lng are geocoded by a
-- separate Airflow task via PostGIS TIGER.
--
-- This model only inserts new addresses. It never updates existing rows.

{{
  config(
    materialized='incremental',
    unique_key='address_hash',
    incremental_strategy='append',
    skip_schema_prefix=true,
    pre_hook=[
      "{% if flags.FULL_REFRESH %}{{ exceptions.raise_compiler_error('geocoding_cache cannot be full-refreshed. Drop it manually if you really mean it.') }}{% endif %}"
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_geocoding_cache_pending ON {{ this }} (address_hash) WHERE latitude IS NULL",
      "CREATE INDEX IF NOT EXISTS idx_geocoding_cache_hash ON {{ this }} (address_hash)"
    ]
  )
}}

with chicago_crimes_addresses as (
    -- Street numbers increase away from State & Madison (41.882037, -87.627804).
    -- The XX redaction rounds street numbers down (to 00), so the true location
    -- is further from that origin than the 00-substituted address. We pick the
    -- coordinate closest to the origin to partially compensate for this bias.
    select
        full_address,
        null::text as street,
        'CHICAGO' as city,
        'IL' as state,
        null::text as zip,
        case
            when avg(latitude) < 41.882037 then min(latitude)
            else max(latitude)
        end as latitude,
        case
            when avg(longitude) >= -87.627804 then min(longitude)
            else max(longitude)
        end as longitude,
        3 as source_address_quality,  -- rounded lat/lng, XX-redacted block addresses
        'chicago_crimes' as source_name
    from {{ ref('stg_chicago_crimes') }}
    group by full_address
),
{# Add more source CTEs here as they're plumbed in, e.g.:
other_source_addresses as (

    select
        full_address,
        street,
        city,
        state,
        zip,
        latitude,
        longitude,
        ingested_at,
        1 as source_address_quality  -- exact addresses with verified lat/lng
    from {{ ref('stg_other_source') }}
    group by full_address, street, city, state, zip
),
#}
unioned as (
    select * from chicago_crimes_addresses
    -- union all
    -- select * from other_source_addresses
),
deduped as (
    select distinct on (
        {{ address_hash(full_address='full_address') }}
    )
        {{ address_hash(full_address='full_address') }} as address_hash,
        full_address as raw_address,
        street,
        city,
        state,
        zip,
        latitude,
        longitude,
        case when latitude is not null then
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        end as geom,
        source_address_quality,
        source_name,
        null::integer as geocode_rating,
        case when latitude is not null then 'source' else null end as geocode_source,
        null::integer as rating_threshold_used,
        null::text as normalized_input,
        null::text as tiger_data_year,
        null::text as tiger_address_num,
        null::text as tiger_predir,
        null::text as tiger_street_name,
        null::text as tiger_street_type,
        null::text as tiger_postdir,
        null::text as tiger_city,
        null::text as tiger_state,
        null::text as tiger_zip,
        now() as created_at,
        null::timestamp as geocoded_at,
        null::timestamp as geocode_attempted_at,
        null::text as geocode_fail_reason
    from unioned
    order by
        {{ address_hash(full_address='full_address') }},
        source_address_quality asc,
        latitude nulls last
)

select * from deduped
{% if is_incremental() %}
where address_hash not in (select address_hash from {{ this }})
{% endif %}
