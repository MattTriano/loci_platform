-- Computes how recently the PIN was previously sold and how soon it
-- will be resold, partitioned by PIN. Both directions are exposed as
-- separate columns; downstream models pick which signal they need.
--
-- Lookback considers all sales (including ineligible ones) because a
-- recent recording event of any kind is meaningful context — a $1
-- quitclaim 30 days before this sale tells you something even though
-- the $1 transaction itself isn't market-rate.
--
-- The 365-day threshold matches the CCAO's model-sales-val convention,
-- but raw days-to-prior-sale and days-to-next-sale are exposed so
-- analysts can pick a different cutoff.
--
-- Tiebreaker on (sale_date, sale_doc_no) ensures deterministic ordering
-- when the same PIN has multiple sales on the same date.

with sales as (

    select * from {{ ref('stg_ccao__parcel_sales') }}

),

with_neighbors as (

    select
        *,

        lag(sale_date) over (
            partition by pin
            order by sale_date, sale_doc_no
        )                                                       as prior_sale_date,

        lead(sale_date) over (
            partition by pin
            order by sale_date, sale_doc_no
        )                                                       as next_sale_date

    from sales

),

flagged as (

    select
        *,

        case
            when prior_sale_date is null then null
            else (sale_date::date - prior_sale_date::date)
        end                                                     as days_since_prior_sale,

        case
            when next_sale_date is null then null
            else (next_sale_date::date - sale_date::date)
        end                                                     as days_until_next_sale,

        -- Within-365 flags. NULL prior/next means "no neighboring sale
        -- in our data" — coalesced to FALSE because we don't want
        -- "no record exists" to read as "yes, there was a recent sale."
        coalesce(
            (sale_date::date - prior_sale_date::date) <= 365,
            false
        )                                                       as has_prior_sale_within_365d,

        coalesce(
            (next_sale_date::date - sale_date::date) <= 365,
            false
        )                                                       as has_next_sale_within_365d

    from with_neighbors

)

select * from flagged