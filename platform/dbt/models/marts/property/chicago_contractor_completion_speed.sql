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
--
-- The issue-window restriction is load-bearing, not just scoping: dated
-- "completions" of old permits are dominated by administrative retouches
-- of long-closed rows (any edit updates socrata_updated_at, so the
-- updated_at_estimate channel selects retouched old permits along with
-- genuine recent completions). Without the restriction, service medians
-- came out in the thousands of days. With it, durations are capped at
-- ~1,095 days by construction and the residual retouch contamination
-- (~2% of the estimated channel, months of error not decades) is a
-- documented caveat. Trade-off to state when reporting: percentiles
-- describe jobs that finished; long-running jobs still open contribute
-- no duration.
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

    select pc.contractor_id, d.permit_, d.duration_days
    from dated_completions d
    join permit_contractors pc on pc.permit_ = d.permit_

),

contractor_service_durations as (

    select distinct cd.contractor_id, t.service, cd.permit_, cd.duration_days
    from contractor_durations cd
    join {{ ref('stg_chicago_building_permit_service_tags') }} t
        on t.permit_ = cd.permit_

)

select
    'contractor' as scope,
    contractor_id,
    null as service,
    count(*) as n_completions,
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
    percentile_cont(0.25) within group (order by duration_days),
    percentile_cont(0.50) within group (order by duration_days),
    percentile_cont(0.75) within group (order by duration_days),
    percentile_cont(0.90) within group (order by duration_days)
from (select distinct service, permit_, duration_days
      from contractor_service_durations) service_durations
group by 1, 2, 3
