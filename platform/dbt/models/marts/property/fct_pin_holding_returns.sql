-- loci_platform/platform/dbt/models/marts/property/fct_pin_holding_returns.sql
{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pair_id'], 'unique': True},
            {'columns': ['pin']},
            {'columns': ['purchase_date']},
            {'columns': ['resale_date']}
        ]
    )
}}

-- One row per consecutive pair of eligible sales on the same PIN, with
-- holding-period economics computed and quality flags attached.
--
-- Returns are nominal (not CPI-adjusted). Real-return analysis requires
-- a separate CPI/HPI adjustment downstream.
--
-- CAGR is NULL for 0-day holds. Short holds (under 90 days) are flagged
-- but their CAGRs are computed normally — short-period annualization
-- mathematically produces large absolute values that are correct but
-- often misleading at human scale; analysts should filter or trust the
-- flag accordingly.
--
-- Pairs that span class changes, intervening filtered sales, or PINs
-- with lifecycle anomalies (apparent splits / consolidations) are
-- flagged but not dropped. Each flag exposes a different kind of
-- "interpret this ROI carefully" signal.

with pairs as (

    select * from {{ ref('int_sales__paired') }}

),

lifecycle as (

    select
        pin,
        n_distinct_classes_observed,
        appears_to_be_new,
        appears_to_have_disappeared
    from {{ ref('int_parcel__lifecycle') }}

),

joined as (

    select
        p.pair_id,
        p.pin,

        -- Pair endpoints
        p.purchase_date,
        p.purchase_price,
        p.purchase_sale_doc_no,
        p.purchase_property_class,
        p.resale_date,
        p.resale_price,
        p.resale_sale_doc_no,
        p.resale_property_class,

        -- Holding period
        p.holding_period_days,
        (p.holding_period_days / 365.25)::numeric(10, 4)        as holding_period_years,

        -- Returns
        ((p.resale_price / p.purchase_price) - 1)::double precision
                                                                as gross_return_pct,

        case
            when p.holding_period_days < 90 then null
            else (
                power(
                    p.resale_price::double precision / p.purchase_price::double precision,
                    365.25 / p.holding_period_days
                ) - 1
            )::double precision
        end                                                     as annualized_cagr_pct,

        -- Flags
        (p.purchase_property_class != p.resale_property_class)  as is_class_changed,
        (p.holding_period_days < 90)                            as is_short_hold,
        (p.holding_period_days >= 1826)                         as is_long_hold,
        (p.n_intervening_filtered_sales > 0)                    as pair_has_intervening_filtered_sales,
        p.n_intervening_filtered_sales,

        -- Lifecycle flags from int_parcel__lifecycle
        coalesce(l.n_distinct_classes_observed, 1)              as pin_n_distinct_classes_observed,
        coalesce(l.appears_to_be_new, false)                    as pin_appears_to_be_new,
        coalesce(l.appears_to_have_disappeared, false)          as pin_appears_to_have_disappeared

    from pairs p
    left join lifecycle l using (pin)

)

select * from joined