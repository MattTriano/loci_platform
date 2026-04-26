{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['neighborhood_code'], 'unique': True},
            {'columns': ['township_code']},
            {'columns': ['triad_code']}
        ]
    )
}}

-- One row per CCAO neighborhood boundary. Grain is town_nbhd
-- (township_code || nbhd) — this is the field that joins to the
-- nbhd column in sales and assessed values. The bare nbhd column
-- is only unique within a township and should not be used as a
-- standalone join key.
--
-- The triad column identifies which third of the county the
-- neighborhood is in (Chicago, North, South). Cook County reassesses
-- one triad per year on a triennial cycle, so triad_code explains
-- why assessed values often hold flat across years for any given
-- parcel — it's only that triad's turn every third year.
--
-- Geometry is preserved as-is (PostGIS multipolygon, EPSG:4326).
-- Spatial operations belong in mart-layer models that use them.
--
-- This source has no SCD columns (no valid_from/valid_to/record_hash);
-- it's a current-snapshot dataset and we take all rows.

with source as (

    select *
    from {{ source('ccao', 'cook_county_neighborhood_boundaries') }}
    where valid_to is null

),

renamed as (

    select
        -- identifier (the field that joins to sales/assessed_values.nbhd)
        town_nbhd                       as neighborhood_code,

        -- decomposed parts
        township_code,
        township_name,
        nbhd                            as neighborhood_within_township,

        -- triennial reassessment grouping
        triad_code,
        triad_name,

        -- geometry
        multipolygon                    as boundary_geom,

        -- ingestion lineage
        ingested_at                     as source_ingested_at

    from source

)

select * from renamed
