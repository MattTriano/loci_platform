-- models/marts/property/chicago_contractor_open_jobs.sql
-- Prong 1: currently open jobs per contractor, from the latest
-- collection snapshot. One row per contractor entity with at least one
-- non-terminal permit.
--
-- "Open" = tracked permit (Express / Renovation / New Construction)
-- classified 'open' in chicago_permit_outcomes: issued, not completed
-- (organically or administratively), not expired/cancelled/revoked, and
-- not suspended. Suspended permits are reported separately rather than
-- folded into open: the transition analysis showed suspension is mostly
-- an administrative holding pen on the way to closure, so most
-- suspended permits are not active jobsites — but some are, so hiding
-- them entirely would understate load.
--
-- Amendment filings are already excluded upstream (outcomes universe).
-- Untracked permit types (signs, demolition, elevator, scaffolding)
-- carry no lifecycle and cannot appear here — stated limitation.
--
-- Attribution: a permit counts as open for every contractor contact on
-- it (gc and trade roles), deduplicated to one count per
-- (contractor, permit).

{{ config(materialized='table') }}

with permit_contractors as (

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

non_terminal as (

    select permit_, issue_date, outcome
    from {{ ref('chicago_permit_outcomes') }}
    where outcome in ('open', 'suspended')

)

select
    pc.contractor_id,
    current_date as as_of,
    count(*) filter (where nt.outcome = 'open') as n_open,
    count(*) filter (where nt.outcome = 'suspended') as n_suspended,
    min(nt.issue_date) filter (where nt.outcome = 'open') as oldest_open_issued,
    percentile_cont(0.5) within group (order by current_date - nt.issue_date)
        filter (where nt.outcome = 'open') as median_open_age_days
from non_terminal nt
join permit_contractors pc on pc.permit_ = nt.permit_
group by 1
