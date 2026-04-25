{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin', 'tax_year'], 'unique': True},
            {'columns': ['tax_year']}
        ]
    )
}}

-- One row per (pin, tax_year). Each row carries the assessed land,
-- building, total, and HIE (Home Improvement Exemption) values at
-- up to three assessment stages: mailed (CCAO initial), certified
-- (CCAO final), and board (BOR post-appeal).
--
-- NULLs in *_bldg / *_land / *_tot / *_hie are meaningful: they mean
-- the PIN did not reach that assessment stage in that year. Do not
-- coalesce or impute here.
--
-- Cook County uses a triennial reassessment cycle by triad (Chicago /
-- north suburbs / south suburbs). A PIN's assessed value typically
-- only changes in its triad's reassessment year — a flat value across
-- years is not a data problem.

with source as (

    select *
    from {{ source('ccao', 'cook_county_assessed_parcel_values') }}
    where valid_to is null

),

renamed as (

    select
        -- identifiers
        lpad(pin, 14, '0')                        as pin,
        cast(year as integer)                     as tax_year,

        -- parcel attributes as-of-year
        class                                     as property_class,
        township_code,
        township_name,
        nbhd                                      as neighborhood_code,

        -- mailed stage (CCAO initial determination)
        mailed_land                               as mailed_land_value,
        mailed_bldg                               as mailed_building_value,
        mailed_tot                                as mailed_total_value,
        mailed_hie                                as mailed_hie_value,

        -- certified stage (CCAO final, post-CCAO-appeals)
        certified_land                            as certified_land_value,
        certified_bldg                            as certified_building_value,
        certified_tot                             as certified_total_value,
        certified_hie                             as certified_hie_value,

        -- board stage (Board of Review, post-BOR-appeals; final for tax year)
        board_land                                as board_land_value,
        board_bldg                                as board_building_value,
        board_tot                                 as board_total_value,
        board_hie                                 as board_hie_value,

        -- ingestion lineage
        row_id                                    as source_row_id,
        ingested_at                               as source_ingested_at

    from source

)

select * from renamed
