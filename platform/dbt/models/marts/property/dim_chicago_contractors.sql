-- models/marts/property/dim_chicago_contractors.sql
-- One row per resolved contractor entity. Names merged by the crosswalk
-- collapse to one row; names not in the crosswalk are their own entity.

with names as (

    select *
    from {{ ref('stg_chicago_building_permit_contractor_names') }}

),

resolved as (

    select
        coalesce(xw.contractor_id, n.name_norm) as contractor_id,
        n.name_norm,
        n.display_name,
        n.permits,
        n.modal_zip,
        n.modal_city,
        n.first_seen,
        n.last_seen
    from names n
    left join {{ source('raw_data', 'contractor_crosswalk') }} xw
        on xw.name_norm = n.name_norm

)

select
    contractor_id,
    -- display name from the member most recently seen on a permit
    (array_agg(display_name order by last_seen desc))[1] as display_name,
    array_agg(name_norm order by permits desc) as member_names,
    sum(permits) as total_permits,
    (array_agg(modal_zip order by permits desc))[1] as modal_zip,
    (array_agg(modal_city order by permits desc))[1] as modal_city,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
from resolved
group by contractor_id
