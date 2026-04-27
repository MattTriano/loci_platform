{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['pin'], 'unique': True}
        ]
    )
}}

-- Per-PIN lifecycle summary derived from staged assessed values. Used
-- by the ROI mart to flag holding periods that span class changes,
-- splits, or consolidations.
--
-- Splits and consolidations are not tracked authoritatively by the CCAO,
-- so we surface only what's cheaply inferrable: a PIN that appears
-- partway through the data ("appears to be new") or disappears before
-- the current max year ("appears to have disappeared"). These are
-- approximate flags, not lineage tracking — they tell you *that*
-- something happened, not *what*.
--
-- Class changes are tracked exactly: n_distinct_classes_observed and
-- the bookend classes are computed from the staged year-by-year record.

with assessed as (

    select
        pin,
        township_code,
        tax_year,
        property_class
    from {{ ref('stg_ccao__assessed_values') }}

),

data_bounds as (

    select
        min(tax_year) as data_min_year,
        max(tax_year) as data_max_year
    from assessed

),

per_pin_aggregates as (

    select
        pin,

        -- township_code can be NULL for some historic records; min()
        -- returns the first non-NULL value, which is at worst arbitrary
        -- and at best deterministic.
        min(township_code)                  as township_code,

        min(tax_year)                       as first_seen_year,
        max(tax_year)                       as last_seen_year,

        count(distinct property_class)      as n_distinct_classes_observed

    from assessed
    group by pin

),

bookend_classes as (

    select
        pin,
        max(case when tax_year = first_seen_year then property_class end) as class_at_first_seen,
        max(case when tax_year = last_seen_year then property_class end)  as class_at_last_seen
    from (
        select
            a.pin,
            a.tax_year,
            a.property_class,
            agg.first_seen_year,
            agg.last_seen_year
        from assessed a
        join per_pin_aggregates agg using (pin)
        where a.tax_year in (agg.first_seen_year, agg.last_seen_year)
    ) t
    group by pin

),

combined as (

    select
        agg.pin,
        agg.township_code,
        agg.first_seen_year,
        agg.last_seen_year,
        agg.n_distinct_classes_observed,
        bk.class_at_first_seen,
        bk.class_at_last_seen,

        agg.first_seen_year > (db.data_min_year + 1)            as appears_to_be_new,
        agg.last_seen_year  < (db.data_max_year - 1)            as appears_to_have_disappeared

    from per_pin_aggregates agg
    join bookend_classes bk using (pin)
    cross join data_bounds db

)

select * from combined
