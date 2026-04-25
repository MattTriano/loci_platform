{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin', 'tax_year', 'card'], 'unique': True},
            {'columns': ['pin', 'tax_year']}
        ]
    )
}}

-- One row per (pin, tax_year, card). A "card" is a distinct building
-- on a parcel; most PINs have exactly one card, but some have more
-- than one (see pin_is_multicard). Joining to sales on just
-- (pin, tax_year) will multiply rows for multi-card parcels — the
-- intermediate layer is responsible for choosing how to collapse cards.
--
-- The tieback_* columns handle the inverse case: one building spanning
-- multiple PINs. card_proration_rate and tieback_proration_rate are
-- the source's allocation rates for splitting value across cards/PINs
-- respectively; not applied here.
--
-- A small number of (pin, year, card) duplicates exist in the source
-- (~10 rows out of ~33M). We deduplicate by keeping the most recently
-- ingested row per grain key; the choice is arbitrary at this scale
-- but deterministic.
--
-- Only cryptic or actively misleading source names are renamed. Most
-- char_* columns are kept as-is because the prefix signals "categorical
-- code — consult CCAO data dictionary."

with source as (

    select distinct on (pin, year, card)
        *
    from {{ source('ccao', 'cook_county_single_and_multi_family_improvement_characteristics') }}
    where valid_to is null
    order by pin, year, card, ingested_at desc

),

renamed as (

    select
        -- identifiers / grain
        lpad(pin, 14, '0')                as pin,
        cast(year as integer)             as tax_year,
        cast(card as integer)             as card,

        -- parcel attributes
        class                             as property_class,
        township_code,

        -- multi-card / multi-land / tieback structure
        pin_is_multicard,
        cast(pin_num_cards as integer)    as pin_num_cards,
        pin_is_multiland,
        cast(pin_num_landlines as integer) as pin_num_landlines,
        tieback_key_pin,
        tieback_proration_rate,
        card_proration_rate,

        -- quality / condition
        cdu,
        char_cnst_qlty,
        char_repair_cnd,
        char_renovation,

        -- size and layout
        cast(char_yrblt as integer)       as year_built,
        char_bldg_sf                      as building_sqft,
        char_land_sf                      as land_sqft,
        cast(char_beds as integer)        as num_bedrooms,
        cast(char_rooms as integer)       as num_rooms,
        cast(char_fbath as integer)       as num_full_baths,
        cast(char_hbath as integer)       as num_half_baths,
        cast(char_frpl as integer)        as num_fireplaces,
        char_apts                         as num_apartments,
        char_type_resd,
        char_use,

        -- construction / exterior / roof
        char_ext_wall,
        char_roof_cnst,

        -- interior systems
        char_heat,
        char_air,

        -- basement / attic / porch
        char_bsmt,
        char_bsmt_fin,
        char_attic_type,
        char_attic_fnsh,
        char_porch,

        -- garage
        char_gar1_att,
        char_gar1_area,
        char_gar1_size,
        char_gar1_cnst,

        -- site / other
        char_site,
        char_ncu,
        char_tp_plan,

        -- ingestion lineage
        row_id                            as source_row_id,
        ingested_at                       as source_ingested_at

    from source

)

select * from renamed
