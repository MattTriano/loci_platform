{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin', 'tax_year'], 'unique': True},
            {'columns': ['pin10']}
        ]
    )
}}

-- One row per (pin, tax_year). Each row is a single condominium PIN —
-- a unit, a deeded parking space, a deeded storage locker, or a
-- common area — within a larger building identified by pin10 (the
-- first 10 digits of the 14-digit PIN).
--
-- pin10 groups all PINs in the same condo building. Within a building,
-- tieback_proration_rate is each PIN's legal share of total ownership
-- and sums to 1.0 across the building (it is what condos use in place
-- of "size" for value allocation).
--
-- is_parking_space, is_common_area, and bldg_is_mixed_use are
-- mart-layer concerns: actual residential condo unit sales typically
-- want is_parking_space = FALSE AND is_common_area = FALSE. Boolean
-- flags may not be stable across years for the same PIN — they appear
-- to reflect current building state.
--
-- Defensive DISTINCT ON applied for grain safety even though no
-- source duplicates were found at last check.

with source as (

    select distinct on (pin, year)
        *
    from {{ source('ccao', 'cook_county_residential_condominium_unit_characteristics') }}
    where valid_to is null
    order by pin, year, ingested_at desc

),

renamed as (

    select
        -- identifiers / grain
        lpad(pin, 14, '0')                  as pin,
        pin10,
        cast(card as integer)               as card,
        cast(year as integer)               as tax_year,

        -- parcel / building attributes
        class                               as property_class,
        township_code,

        -- building grouping (which PINs belong to the same building)
        tieback_key_pin,
        tieback_proration_rate,
        card_proration_rate,

        -- unit type indicators
        is_parking_space,
        is_common_area,
        bldg_is_mixed_use,

        -- multi-land structure (rare for condos but preserved)
        pin_is_multiland,
        cast(pin_num_landlines as integer)  as pin_num_landlines,

        -- size / vintage
        cast(char_yrblt as integer)         as year_built,
        char_building_sf                    as building_sqft,
        char_unit_sf                        as unit_sqft,
        char_land_sf                        as land_sqft,
        cast(char_bedrooms as integer)      as num_bedrooms,
        cast(char_full_baths as integer)    as num_full_baths,
        cast(char_half_baths as integer)    as num_half_baths,

        -- building composition
        cast(char_building_non_units as integer) as building_num_non_units,
        cast(char_building_pins as integer)      as building_num_pins,

        -- condition
        cdu,

        -- ingestion lineage
        row_id                              as source_row_id,
        ingested_at                         as source_ingested_at

    from source

)

select * from renamed
