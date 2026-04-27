-- Composes the CCAO's pre-computed sale filter flags plus the multisale
-- indicator into a single eligibility boolean. This is the gateway every
-- subsequent flag model passes through.
--
-- A sale is "eligible for market analysis" when none of the four
-- disqualifying conditions fire. This reconstructs the pre-October-2023
-- behavior of the CCAO's published Parcel Sales dataset, which was
-- pre-filtered by these same rules before they stopped filtering and
-- shifted that responsibility to consumers.
--
-- Eligibility is a necessary precondition for arms-length analysis but
-- not a sufficient one. Subsequent flag models layer on outlier, short-
-- term-resale, and name-based heuristics.
--
-- NULL flags are treated as non-disqualifying — a NULL means CCAO did
-- not compute that filter for the row, not that the row is suspect. The
-- alternative (treating NULL as suspect) would silently drop rows for
-- the wrong reason.

with sales as (

    select * from {{ ref('stg_ccao__parcel_sales') }}

),

flagged as (

    select
        *,

        -- Disqualifying conditions, named so each scans as part of the suite.
        -- Multisale is the structural disqualifier (price not attributable
        -- to one parcel); the ccao_flag_* columns are CCAO's heuristic
        -- disqualifiers carried in from staging.
        is_multisale                                            as is_multisale_disqualifying,

        -- Composite eligibility: TRUE only when none of the four
        -- disqualifiers fire. coalesce(..., false) treats NULL flags as
        -- non-disqualifying (see model description).
        not coalesce(is_multisale, false)
            and not coalesce(ccao_flag_deed_type, false)
            and not coalesce(ccao_flag_under_10k, false)
            and not coalesce(ccao_flag_duplicate_price_365d, false)
                                                                as is_eligible_for_market_analysis

    from sales

)

select * from flagged
