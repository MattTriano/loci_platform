-- loci_platform/platform/dbt/models/intermediate/ccao/int_sales__paired.sql
{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin']},
            {'columns': ['purchase_date']},
            {'columns': ['resale_date']}
        ]
    )
}}

-- One row per consecutive pair of eligible sales on the same PIN.
-- A PIN with N eligible sales produces N-1 pair rows.
--
-- Pair endpoints are restricted to eligible sales (passed the CCAO
-- pre-filters and is_multisale check). Sales that fail eligibility
-- are not used as endpoints — but their existence between two
-- eligible sales is surfaced as n_intervening_filtered_sales, which
-- is a quality flag for the pair: a non-zero value means the
-- chain-of-title between purchase and resale included transactions
-- we filtered out (often $1 quitclaims, family transfers, foreclosure
-- steps), and the ROI should be interpreted skeptically.
--
-- Same-day same-PIN sales are treated as separate events ordered by
-- sale_doc_no, producing 0-day-holding-period pairs.

with all_sales as (

    select
        source_row_id,
        pin,
        sale_doc_no,
        sale_date,
        sale_price,
        property_class,
        is_eligible_for_market_analysis as is_eligible
    from {{ ref('int_sales__flag_pre_filters') }}

),

with_eligible_seq as (

    -- Number sales globally per PIN, and number eligible sales separately.
    -- Ineligible sales between two eligible sales will share the eligible
    -- sequence number of the most recent prior eligible sale.
    select
        *,
        row_number() over (
            partition by pin
            order by sale_date, sale_doc_no
        ) as global_seq,
        sum(case when is_eligible then 1 else 0 end) over (
            partition by pin
            order by sale_date, sale_doc_no
            rows between unbounded preceding and current row
        ) as eligible_seq
    from all_sales

),

ineligible_counts as (

    -- For each "eligible position" (eligible_seq value), count how many
    -- ineligible sales occurred AFTER that eligible sale but before the
    -- next one. We do this by counting ineligibles whose eligible_seq
    -- equals N (i.e., they came after eligible #N) and grouping.
    select
        pin,
        eligible_seq                            as after_eligible_seq,
        count(*)                                as n_ineligible_in_window
    from with_eligible_seq
    where not is_eligible
      and eligible_seq >= 1                     -- exclude ineligibles before any eligible sale
    group by pin, eligible_seq

),

eligible_only as (

    select
        source_row_id,
        pin,
        sale_doc_no,
        sale_date,
        sale_price,
        property_class,
        eligible_seq
    from with_eligible_seq
    where is_eligible

),

paired as (

    select
        purchase.pin                                            as pin,

        purchase.source_row_id                                  as purchase_source_row_id,
        purchase.sale_doc_no                                    as purchase_sale_doc_no,
        purchase.sale_date                                      as purchase_date,
        purchase.sale_price                                     as purchase_price,
        purchase.property_class                                 as purchase_property_class,

        resale.source_row_id                                    as resale_source_row_id,
        resale.sale_doc_no                                      as resale_sale_doc_no,
        resale.sale_date                                        as resale_date,
        resale.sale_price                                       as resale_price,
        resale.property_class                                   as resale_property_class,

        (resale.sale_date::date - purchase.sale_date::date)     as holding_period_days,

        coalesce(ic.n_ineligible_in_window, 0)::integer         as n_intervening_filtered_sales

    from eligible_only purchase
    inner join eligible_only resale
        on purchase.pin = resale.pin
        and purchase.eligible_seq = resale.eligible_seq - 1
    left join ineligible_counts ic
        on ic.pin = purchase.pin
        and ic.after_eligible_seq = purchase.eligible_seq

),

with_pair_id as (

    select
        purchase_sale_doc_no || ':' || resale_sale_doc_no       as pair_id,
        *
    from paired

)

select * from with_pair_id
