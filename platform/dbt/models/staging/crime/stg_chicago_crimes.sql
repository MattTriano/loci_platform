with source as (
    select
        *,
        regexp_replace({{ clean_address('block') }}, 'XX', '00', 'g') as cleaned_block
    from {{ source('raw_data', 'chicago_crimes') }}
    where valid_to is null
),
cleaned as (
    select
        id,
        case_number,
        date,
        block,
        cleaned_block,
        cleaned_block || ', CHICAGO, IL' as full_address,
        iucr,
        primary_type,
        description,
        location_description,
        arrest,
        domestic,
        beat,
        district,
        ward,
        community_area,
        fbi_code,
        year,
        updated_on,
        round(latitude::numeric, 3)::double precision as latitude,
        round(longitude::numeric, 3)::double precision as longitude,
        location,
        ingested_at,
        socrata_id,
        socrata_updated_at,
        socrata_created_at,
        socrata_version,
        record_hash,
        valid_from
    from source

)

select * from cleaned
