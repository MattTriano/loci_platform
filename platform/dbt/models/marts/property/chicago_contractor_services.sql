-- models/marts/property/chicago_contractor_services.sql
-- One row per (contractor entity, service, evidence channel), with the
-- activity window for that service so consumers can filter to recent
-- offerings without this model taking a position on the window.
--
-- Attribution rule: a permit's description/type describes the PROJECT,
-- not every contact on it — so description-channel services attach only
-- to general contractors; trade contractors get their service from
-- their declared role, which applies regardless of description.

{{ config(materialized='table') }}

with contacts as (

    select
        c.permit_,
        c.issue_date,
        m.role,
        m.service as role_service,
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

trade_roles as (

    select
        contractor_id,
        role_service as service,
        count(distinct permit_) as permits,
        min(issue_date) as first_seen,
        max(issue_date) as last_seen
    from contacts
    where role = 'trade'
    group by 1, 2

),

gc_project_services as (

    select
        c.contractor_id,
        t.service,
        count(distinct c.permit_) as permits,
        min(c.issue_date) as first_seen,
        max(c.issue_date) as last_seen
    from contacts c
    join {{ ref('stg_chicago_building_permit_service_tags') }} t
        on t.permit_ = c.permit_
    where c.role = 'gc'
    group by 1, 2

)

select contractor_id, service, permits, first_seen, last_seen,
       'trade_role' as evidence
from trade_roles

union all

select contractor_id, service, permits, first_seen, last_seen,
       'description' as evidence
from gc_project_services
