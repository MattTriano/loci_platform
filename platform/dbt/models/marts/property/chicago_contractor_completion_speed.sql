-- models/marts/property/chicago_contractor_completion_speed.sql
-- Completion-speed percentiles (p25 / median / p75 / p90, with counts)
-- at three grains, distinguished by the scope column:
--   contractor         - per contractor, all their dated completions
--   contractor_service - per contractor per service
--   service            - per service, all contractors
--
-- Population: permits ISSUED within the 3-year analysis window that have
-- a dated completion (outcome complete_organic or complete_estimated)
-- falling on/after 2025-10-14 (the earliest recoverable completion date).
-- The issue-window restriction is load-bearing: without it, dated
-- "completions" of old permits (administrative retouches of long-closed
-- rows) dominated and medians came out in the thousands of days.
--
-- pct_observed is a confidence signal: the share of a row's durations
-- coming from directly observed SCD2 transitions rather than
-- updated_at-based estimates. Source-agreement analysis (2026-07) found
-- the two channels agree closely for high-volume services and the
-- estimated channel skews ~1-3 months long for slower, low-volume
-- services due to residual retouch contamination; rows with low
-- pct_observed and long medians should be read with that in mind. The
-- observed share grows with every collection cycle.
--
-- Attribution: a permit's duration is attributed to every contractor
-- contact on it (gc and trade roles). Service grain comes from the
-- permit's service tags; a multi-tagged permit contributes to each of
-- its services. The contractor grain deduplicates to one duration per
-- (contractor, permit).

{{ config(materialized='table') }}

with dated_completions as (

    select permit_, duration_days, completion_source
    from {{ ref('chicago_permit_outcomes') }}
    where outcome in ('complete_organic', 'complete_estimated')
      and completed_at >= date '2025-10-14'
      and issue_date >= current_date - interval '3 years'

),

permit_contractors as (

    select distinct
        c.permit_,
        coalesce(xw.contractor_id, c.name_norm) as contractor_id
    from {{ ref('stg_chicago_building_permit_contacts') }} c
    join {{ ref('stg_chicago_building_permit_contact_role_map') }} m
        on m.contact_type = c.contact_type
    left join {{ source('raw_data', 'contractor_crosswalk') }} xw
        on xw.name_norm = c.name_norm
    where m.role in ('gc', 'trade')
      and c.name_norm is not null
      and c.name_norm not like 'OWNER ACTING%'

),

contractor_durations as (

    select pc.contractor_id, d.permit_, d.duration_days, d.completion_source
    from dated_completions d
    join permit_contractors pc on pc.permit_ = d.permit_

),

contractor_service_durations as (

    select distinct
        cd.contractor_id, t.service, cd.permit_,
        cd.duration_days, cd.completion_source
    from contractor_durations cd
    join {{ ref('stg_chicago_building_permit_service_tags') }} t
        on t.permit_ = cd.permit_

)

select
    'contractor' as scope,
    contractor_id,
    null as service,
    count(*) as n_completions,
    round(100.0 * count(*) filter (where completion_source = 'observed_transition')
          / count(*), 1) as pct_observed,
    percentile_cont(0.25) within group (order by duration_days) as p25_days,
    percentile_cont(0.50) within group (order by duration_days) as median_days,
    percentile_cont(0.75) within group (order by duration_days) as p75_days,
    percentile_cont(0.90) within group (order by duration_days) as p90_days
from contractor_durations
group by 1, 2, 3

union all

select
    'contractor_service',
    contractor_id,
    service,
    count(*),
    round(100.0 * count(*) filter (where completion_source = 'observed_transition')
          / count(*), 1),
    percentile_cont(0.25) within group (order by duration_days),
    percentile_cont(0.50) within group (order by duration_days),
    percentile_cont(0.75) within group (order by duration_days),
    percentile_cont(0.90) within group (order by duration_days)
from contractor_service_durations
group by 1, 2, 3

union all

select
    'service',
    null,
    service,
    count(*),
    round(100.0 * count(*) filter (where completion_source = 'observed_transition')
          / count(*), 1),
    percentile_cont(0.25) within group (order by duration_days),
    percentile_cont(0.50) within group (order by duration_days),
    percentile_cont(0.75) within group (order by duration_days),
    percentile_cont(0.90) within group (order by duration_days)
from (select distinct service, permit_, duration_days, completion_source
      from contractor_service_durations) service_durations
group by 1, 2, 3
