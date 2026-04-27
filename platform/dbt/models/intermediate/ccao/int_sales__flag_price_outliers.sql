-- loci_platform/platform/dbt/models/intermediate/ccao/sales_flags/int_sales__flag_price_outliers.sql
{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin']},
            {'columns': ['sale_doc_no']},
            {'columns': ['source_row_id'], 'unique': True}
        ]
    )
}}

-- Flags sales whose price is unusually high or low relative to peers in
-- the same neighborhood (or township, as fallback) x property_class x sale_year.
--
-- Statistics (median, IQR) are computed over eligible sales only --
-- multisales, sub-$10k transfers, and non-warranty deeds don't represent
-- market-clearing prices and would pollute the reference distribution.
-- All sales are scored against that eligible-sales distribution, so an
-- ineligible sale that's also a price outlier carries reinforced signal.
--
-- Geographic resolution falls back hierarchically:
--   1. neighborhood x class x year, if eligible n >= 20
--   2. township x class x year, if eligible n >= 20
--   3. otherwise flags are NULL and is_in_thin_group = TRUE
-- The peer_group_level column records which level was used.
--
-- Outlier statistic is a robust z-score on log(sale_price):
--   (log_price - group_median) / (group_IQR / 1.349)
-- IQR/1.349 is the consistent estimator of sigma for a normal distribution,
-- analogous to MAD/0.6745 but computable in a single pass with quartiles.
-- Log transform handles the right-skew of sale prices; IQR-based scale is
-- robust to the outliers we're trying to flag (unlike std-dev, which they
-- would inflate and mask themselves with).
--
-- Flag threshold |z| >= 3.5 follows the Iglewicz & Hoaglin convention.
-- The raw z-score is exposed so downstream consumers can pick their own
-- threshold without re-running this model.

with base as (

    select
        source_row_id,
        pin,
        sale_doc_no,
        sale_date,
        sale_price,
        property_class,
        township_code,
        neighborhood_code,
        is_eligible_for_market_analysis,
        extract(year from sale_date)::integer       as sale_year,
        ln(sale_price::double precision)            as log_price
    from {{ ref('int_sales__flag_pre_filters') }}
    where sale_price > 0  -- ln() undefined at zero; ineligible sales
                          -- with $0 price are dropped from this model

),

-- ---------- Neighborhood-level statistics (one pass) ----------

nbhd_stats as (

    select
        neighborhood_code,
        property_class,
        sale_year,
        count(*)                                                            as n_eligible,
        percentile_disc(0.5)  within group (order by log_price)             as p50_log_price,
        percentile_disc(0.25) within group (order by log_price)             as p25_log_price,
        percentile_disc(0.75) within group (order by log_price)             as p75_log_price
    from base
    where is_eligible_for_market_analysis
      and neighborhood_code is not null
    group by neighborhood_code, property_class, sale_year

),

-- ---------- Township-level statistics (fallback, one pass) ----------

twp_stats as (

    select
        township_code,
        property_class,
        sale_year,
        count(*)                                                            as n_eligible,
        percentile_disc(0.5)  within group (order by log_price)             as p50_log_price,
        percentile_disc(0.25) within group (order by log_price)             as p25_log_price,
        percentile_disc(0.75) within group (order by log_price)             as p75_log_price
    from base
    where is_eligible_for_market_analysis
      and township_code is not null
    group by township_code, property_class, sale_year

),

-- ---------- Score every sale, falling back as needed ----------

scored as (

    select
        b.*,

        case
            when ns.n_eligible >= 20 then 'neighborhood'
            when ts.n_eligible >= 20 then 'township'
            else 'thin'
        end                                                                 as peer_group_level,

        case
            when ns.n_eligible >= 20 then ns.n_eligible
            when ts.n_eligible >= 20 then ts.n_eligible
            else coalesce(ns.n_eligible, ts.n_eligible)
        end                                                                 as peer_group_n_eligible,

        case
            when ns.n_eligible >= 20 then ns.p50_log_price
            when ts.n_eligible >= 20 then ts.p50_log_price
            else null
        end                                                                 as peer_group_median_log_price,

        case
            when ns.n_eligible >= 20 then (ns.p75_log_price - ns.p25_log_price)
            when ts.n_eligible >= 20 then (ts.p75_log_price - ts.p25_log_price)
            else null
        end                                                                 as peer_group_iqr_log_price

    from base b
    left join nbhd_stats ns
        on  b.neighborhood_code = ns.neighborhood_code
        and b.property_class    = ns.property_class
        and b.sale_year         = ns.sale_year
    left join twp_stats ts
        on  b.township_code     = ts.township_code
        and b.property_class    = ts.property_class
        and b.sale_year         = ts.sale_year

),

flagged as (

    select
        *,

        -- Robust z-score. NULL when the chosen group's IQR is zero
        -- (degenerate group: middle 50% of eligible sales identically priced)
        -- or when no group reached the n>=20 threshold.
        case
            when peer_group_iqr_log_price is null then null
            when peer_group_iqr_log_price = 0     then null
            else (log_price - peer_group_median_log_price) / (peer_group_iqr_log_price / 1.349)
        end                                                                 as price_modified_z_score,

        (peer_group_level = 'thin')                                         as is_in_thin_group
    from scored
),
final as (
    select
        *,
        case
            when price_modified_z_score is null then null
            else price_modified_z_score >= 3.5
        end                                                                     as is_price_high_outlier,
        case
            when price_modified_z_score is null then null
            else price_modified_z_score <= -3.5
        end                                                                     as is_price_low_outlier
    from flagged
)

select * from final
