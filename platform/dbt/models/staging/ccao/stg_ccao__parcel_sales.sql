-- One row per sale-document per PIN. A deed covering multiple PINs
-- (is_multisale = TRUE) produces one row per PIN with the deed's
-- total price replicated across them — do NOT sum sale_price across
-- multisale rows.
--
-- We filter to current SCD records (valid_to IS NULL) here so every
-- downstream model can assume point-in-time correctness.

with source as (
    select *
    from {{ source('ccao', 'cook_county_parcel_sales') }}
    where valid_to is null
),
renamed as (

    select
        -- identifiers
        lpad(pin, 14, '0')                          as pin,
        doc_no                                      as sale_doc_no,
        row_id                                      as source_row_id,

        -- parcel attributes as-of-sale (denormalized snapshot from source)
        township_code,
        nbhd                                        as neighborhood_code,
        class                                       as property_class,

        -- time
        sale_date,
        is_mydec_date                               as sale_date_is_from_mydec,
        cast(year as integer)                       as tax_year,

        -- price
        cast(sale_price as numeric(14, 2))          as sale_price,

        -- deed / transaction attributes
        deed_type,
        mydec_deed_type,
        coalesce(mydec_deed_type, deed_type)        as deed_type_preferred,
        sale_type,
        seller_name,
        buyer_name,

        -- multisale indicators
        is_multisale,
        cast(num_parcels_sale as integer)           as num_parcels_on_deed,

        -- CCAO's own pre-computed heuristic filter flags
        -- (TRUE means the CCAO considers the sale suspect for that reason)
        sale_filter_same_sale_within_365            as ccao_flag_duplicate_price_365d,
        sale_filter_less_than_10k                   as ccao_flag_under_10k,
        sale_filter_deed_type                       as ccao_flag_deed_type,

        -- ingestion lineage (kept for debugging; not used downstream)
        ingested_at                                 as source_ingested_at

    from source
),
deduped as (
    select
        *,
        row_number() over (
            partition by pin, sale_doc_no
            order by
                -- 1. Prefer MyDec-sourced dates (real day precision)
                sale_date_is_from_mydec desc,
                -- 2. Prefer rows where tax_year matches the sale_date year
                case
                    when extract(year from sale_date) = tax_year then 0
                    else 1
                end asc,
                -- 3. Prefer rows with more populated fields
                (
                    (case when buyer_name  is not null then 1 else 0 end) +
                    (case when seller_name is not null then 1 else 0 end) +
                    (case when sale_type   is not null then 1 else 0 end) +
                    (case when mydec_deed_type is not null then 1 else 0 end)
                ) desc,
                -- 4. Prefer the latest sale_date among remaining ties
                --    (full dates beat first-of-month/year fallbacks)
                sale_date desc nulls last,
                -- 5. Stable deterministic tiebreaker
                source_row_id
        ) as sale_dedupe_rank
    from renamed
),
final as (
    select
        pin,
        sale_doc_no,
        source_row_id,
        township_code,
        neighborhood_code,
        property_class,
        sale_date,
        sale_date_is_from_mydec,
        tax_year,
        sale_price,
        deed_type,
        mydec_deed_type,
        deed_type_preferred,
        sale_type,
        seller_name,
        buyer_name,
        is_multisale,
        num_parcels_on_deed,
        ccao_flag_duplicate_price_365d,
        ccao_flag_under_10k,
        ccao_flag_deed_type,
        source_ingested_at
    from deduped
    where sale_dedupe_rank = 1
)

select * from final
