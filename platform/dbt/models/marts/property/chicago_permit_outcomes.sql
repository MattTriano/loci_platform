-- models/marts/property/chicago_permit_outcomes.sql
-- One row per tracked permit (Express / Renovation / New Construction —
-- the types that carry status) with a classified outcome, and where a
-- completion date is recoverable, the date, its source, and the duration.
--
-- Completion-date sources, in priority order:
--   observed_transition: the SCD2 version log captured the COMPLETE flip
--     (collection began 2026-03-04; dates accurate to the twice-weekly
--     cadence, ~±4 days).
--   updated_at_estimate: permit was already COMPLETE at first collection,
--     and its socrata_updated_at postdates the portal's 2025-10-14 full
--     restamp — validated retouch rate ~2.2%, so the last-touch date
--     approximates the completion flip.
--   Completions before 2025-10-14 are undatable (restamped) and appear
--     as complete_undated.
--
-- Organic vs administrative completion: the transition analysis showed
-- completions arriving from inspections-family states are real
-- inspection closures, while INSPECTION ELIGIBLE -> COMPLETE (~397 days,
-- the 13-month auto-close sweep) and SUSPENDED -> COMPLETE (median ~797
-- days, backlog cleanup) are administrative and carry no work-finish
-- information. For recovered Express completions (no prior state
-- observable), duration > 365 days is classified administrative, since
-- observed Express organic completions overwhelmingly occur inside the
-- one-year validity window and both sweep paths occur beyond it.

{{ config(materialized='table') }}

with versions as (

    select
        permit_,
        permit_milestone,
        valid_from,
        lag(permit_milestone) over (
            partition by permit_ order by valid_from
        ) as prev_milestone
    from {{ source('raw_data', 'chicago_building_permits') }}

),

observed_flips as (

    select distinct on (permit_)
        permit_,
        valid_from::date as completed_at,
        prev_milestone
    from versions
    where permit_milestone = 'COMPLETE'
      and prev_milestone is distinct from 'COMPLETE'
      and prev_milestone is not null
    order by permit_, valid_from

),

current_permits as (

    select
        permit_,
        permit_type,
        issue_date::date as issue_date,
        permit_status,
        permit_milestone,
        socrata_updated_at::date as last_touched
    from {{ source('raw_data', 'chicago_building_permits') }}
    where valid_to is null
      and permit_status is not null

),

classified as (

    select
        p.permit_,
        p.permit_type,
        p.issue_date,
        p.permit_status,
        p.permit_milestone,

        case
            -- observed flip, from an inspections-family state: real closure
            when f.permit_ is not null
                 and f.prev_milestone not in ('INSPECTION ELIGIBLE', 'SUSPENDED')
                then 'complete_organic'

            -- observed flip via the auto-close or backlog-sweep paths
            when f.permit_ is not null
                then 'complete_admin'

            -- pre-collection completion, dated via post-restamp last touch
            when p.permit_milestone in ('COMPLETE', 'CERTIFICATE OF OCCUPANCY ISSUED')
                 and p.last_touched > date '2025-10-14'
                then case
                    when p.permit_type = 'PERMIT – EXPRESS PERMIT PROGRAM'
                         and p.last_touched - p.issue_date > 365
                        then 'complete_admin'
                    else 'complete_estimated'
                end

            -- completed sometime before the restamp: date unrecoverable
            when p.permit_milestone in ('COMPLETE', 'CERTIFICATE OF OCCUPANCY ISSUED')
                then 'complete_undated'

            when p.permit_status = 'EXPIRED'   then 'expired'
            when p.permit_status = 'CANCELLED' then 'cancelled'
            when p.permit_status = 'REVOKED'   then 'revoked'
            when p.permit_status = 'SUSPENDED' then 'suspended'
            else 'open'
        end as outcome,

        coalesce(f.completed_at, p.last_touched) as candidate_completed_at,
        case when f.permit_ is not null then 'observed_transition'
             else 'updated_at_estimate' end as candidate_source

    from current_permits p
    left join observed_flips f on f.permit_ = p.permit_

)

select
    permit_,
    permit_type,
    issue_date,
    permit_status,
    permit_milestone,
    outcome,

    -- date and duration only where the outcome makes them meaningful
    case when outcome in ('complete_organic', 'complete_estimated')
         then candidate_completed_at end as completed_at,
    case when outcome in ('complete_organic', 'complete_estimated')
         then candidate_source end as completion_source,
    case when outcome in ('complete_organic', 'complete_estimated')
         then candidate_completed_at - issue_date end as duration_days

from classified
