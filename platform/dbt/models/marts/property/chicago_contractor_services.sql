-- models/marts/property/chicago_contractor_services.sql
-- One row per (contractor entity, service, evidence channel).
--
-- Attribution rule: a permit's description describes the PROJECT, not
-- every contact on it — the electrical sub on a gut rehab doesn't offer
-- roofing. So description-derived services attach only to the general
-- contractor; trade-specific contractors get their service from their
-- declared role, which applies regardless of description.

{{ config(materialized='table') }}

with contacts as (

    select
        c.permit_,
        c.contact_type,
        coalesce(xw.contractor_id, c.name_norm) as contractor_id
    from {{ ref('stg_chicago_building_permit_contacts') }} c
    left join {{ source('raw_data', 'contractor_crosswalk') }} xw
        on xw.name_norm = c.name_norm
    where c.contact_type ilike '%CONTRACTOR%'
      and c.name_norm is not null
      and c.name_norm not like 'OWNER ACTING%'

),

-- Explicit contact_type -> service mapping. Unmapped trade types fall
-- through (and are surfaced by the step-4 audit query, not lost silently).
trade_type_map (contact_type_pattern, service) as (

    values
        ('%GENERAL%',    null),          -- GCs get services from descriptions
        ('%ELECTRIC%',   'electrical'),
        ('%PLUMB%',      'plumbing'),
        ('%MASON%',      'masonry'),
        ('%HVAC%',       'hvac'),
        ('%REFRIGERAT%', 'hvac'),
        ('%VENTILAT%',   'hvac')

),

trade_roles as (

    select c.contractor_id, m.service, count(distinct c.permit_) as permits
    from contacts c
    join trade_type_map m on c.contact_type ilike m.contact_type_pattern
    where m.service is not null
    group by 1, 2

),

gc_description_services as (

    select c.contractor_id, t.service, count(distinct c.permit_) as permits
    from contacts c
    join {{ ref('stg_chicago_building_permit_service_tags') }} t
        on t.permit_ = c.permit_
    where c.contact_type ilike '%GENERAL%'
    group by 1, 2

)

select contractor_id, service, permits, 'trade_role' as evidence
from trade_roles
union all
select contractor_id, service, permits, 'description' as evidence
from gc_description_services
