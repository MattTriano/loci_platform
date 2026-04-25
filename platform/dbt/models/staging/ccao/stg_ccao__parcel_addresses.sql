{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin', 'tax_year'], 'unique': True},
            {'columns': ['pin10']}
        ]
    )
}}

-- One row per (pin, tax_year). Each row carries the situs address
-- (prop_address_*, the parcel's physical location) and the mailing
-- address (mail_address_*, where tax bills are sent).
--
-- Mailing address ≠ situs address is a strong signal that the parcel
-- is non-owner-occupied (rental, trust-held, business-held). The
-- mail_address_name field carries the recipient as written on the
-- tax roll, often containing entity keywords (LLC, TRUST, INC, etc.)
-- that downstream name heuristics will use.
--
-- Addresses are not normalized here — fields are passed through as
-- recorded, with the inconsistencies that implies (abbreviated
-- street types, mixed case, embedded suffixes). Address standardization
-- is its own discipline and belongs in a dedicated downstream step
-- if and when needed.

with source as (

    select distinct on (pin, year)
        *
    from {{ source('ccao', 'cook_county_parcel_addresses') }}
    where valid_to is null
    order by pin, year, ingested_at desc

),

renamed as (

    select
        -- identifiers / grain
        lpad(pin, 14, '0')              as pin,
        pin10,
        cast(year as integer)           as tax_year,

        -- situs address (physical location of the parcel)
        prop_address_full               as situs_address_full,
        prop_address_city_name          as situs_city,
        prop_address_state              as situs_state,
        prop_address_zipcode_1          as situs_zip5,

        -- mailing address (where tax bills are sent)
        mail_address_name,
        mail_address_full,
        mail_address_city_name          as mail_city,
        mail_address_state              as mail_state,
        mail_address_zipcode_1          as mail_zip5,

        -- ingestion lineage
        row_id                          as source_row_id,
        ingested_at                     as source_ingested_at

    from source

)

select * from renamed
